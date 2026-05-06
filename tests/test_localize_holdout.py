#!/usr/bin/env python3
"""Holdout validation for single-image localization.

Takes a completed local refinement output (reconstruction + features.h5),
holds out a fraction of images, and localizes them against the remainder.
Reports pose error statistics.

Usage:
    python tests/test_localize_holdout.py \
        --scan_sfm_dir refined/local/<scan_id>/sfm \
        --image_dir datasets/<scan_id>/Frames/ \
        --holdout_fraction 0.2 \
        --output_dir tests/localize_holdout_results/

Test protocol -- run three times:
    1. Easy:      --noise_m 0.0   --noise_deg 0.0   (pipeline sanity)
    2. Realistic: --noise_m 0.5   --noise_deg 5.0   (ARKit-level drift)
    3. Stress:    --noise_m 2.0   --noise_deg 15.0   (graceful degradation)

``holdout_results.json`` includes ``correlation_vs_pose`` (Pearson/Spearman of
errors vs ``num_inliers`` / ``num_matches`` / ``num_2d3d``) when enough
successful rows exist. A scatter figure ``holdout_correlation_scatter.png`` is
written to the same directory (needs matplotlib). Recompute from an existing JSON::

    python tests/test_localize_holdout.py --analyze_only tests/localize_holdout_results/holdout_results.json
    python tests/test_localize_holdout.py --analyze_only tests/localize_holdout_results/holdout_results.json --min_inliers_2d3d_ratio 0.25

QC threshold sweep (default: BAD = ``pos_error_m`` > ``--qc_good_max_pos_m`` OR
``rot_error_deg`` > ``--qc_good_max_rot_deg``)::

    python tests/test_localize_holdout.py --threshold_sweep tests/localize_holdout_results/holdout_results.json
    python tests/test_localize_holdout.py --analyze_only tests/localize_holdout_results/holdout_results.json --qc_good_max_pos_m 0.15 --qc_good_max_rot_deg 3
"""

from pathlib import Path
import argparse
import json
import logging
import math
import sys
from typing import Callable, List, Optional, Tuple
import numpy as np
import torch # import torch before pycolmap to avoid a crash on macOS
import pycolmap
from copy import deepcopy
from scipy import stats
from scipy.spatial.transform import Rotation as ScipyRotation

# Add project root so imports work
sys.path.insert(0, str(Path(__file__).parent.parent))

from localize_image import SingleImageLocalizer

logger = logging.getLogger("holdout_test")


def _first_stat(res) -> float:
    """Pearsonr / Spearmanr return object or (stat, pvalue) tuple across scipy versions."""
    if hasattr(res, "statistic"):
        return float(res.statistic)
    if hasattr(res, "correlation"):
        return float(res.correlation)
    return float(res[0])


def correlation_vs_pose_metrics(per_image: list) -> dict:
    """Pearson / Spearman between pose errors and match statistics.

    Returns empty dict if fewer than 3 successful rows with pose errors.
    """
    packed = _correlation_plot_rows(per_image)
    if packed is None:
        return {}
    pos_cm, rot_deg, metrics = packed
    n = len(pos_cm)
    inl, mtc, d3 = metrics[0][1], metrics[1][1], metrics[2][1]
    ratio = inl / np.maximum(d3, 1.0)

    metrics_json = [
        ("num_inliers", inl),
        ("num_matches", mtc),
        ("num_2d3d", d3),
        ("num_inliers_div_num_2d3d", metrics[3][1]),
    ]
    targets = [
        ("pos_error_cm", pos_cm),
        ("rot_error_deg", rot_deg),
    ]

    pearson = {}
    spearman = {}
    for tn, tv in targets:
        pearson[tn] = {}
        spearman[tn] = {}
        for mn, mv in metrics_json:
            pearson[tn][mn] = _first_stat(stats.pearsonr(tv, mv))
            spearman[tn][mn] = _first_stat(stats.spearmanr(tv, mv))

    k = max(1, n // 4)
    order = np.argsort(pos_cm)[::-1]
    worst_idx, best_idx = order[:k], order[-k:]

    def _means(idxs):
        return {
            "num_inliers": float(np.mean(inl[idxs])),
            "num_matches": float(np.mean(mtc[idxs])),
            "num_2d3d": float(np.mean(d3[idxs])),
            "num_inliers_div_num_2d3d": float(np.mean(ratio[idxs])),
        }

    return {
        "n": n,
        "description": (
            "Pearson/Spearman vs pose error; negative => higher counts "
            "associate with lower error. num_inliers_div_num_2d3d is inlier "
            "count divided by num_2d3d."
        ),
        "pearson": pearson,
        "spearman": spearman,
        "quartiles_pos_error_cm": {
            "worst_k": k,
            "worst_mean_match_stats": _means(worst_idx),
            "best_mean_match_stats": _means(best_idx),
        },
    }


def _correlation_plot_rows(
    per_image: list,
) -> Optional[Tuple[np.ndarray, np.ndarray, List[Tuple[str, np.ndarray]]]]:
    """Return (pos_cm, rot_deg, metrics_list) or None if too few points."""
    rows = [
        r
        for r in per_image
        if r.get("success")
        and r.get("pos_error_m") is not None
        and r.get("rot_error_deg") is not None
    ]
    if len(rows) < 3:
        return None
    pos_cm = np.array([r["pos_error_m"] * 100.0 for r in rows], dtype=np.float64)
    rot_deg = np.array([r["rot_error_deg"] for r in rows], dtype=np.float64)
    inl = np.array([r["num_inliers"] for r in rows], dtype=np.float64)
    mtc = np.array([r["num_matches"] for r in rows], dtype=np.float64)
    d3 = np.array([r["num_2d3d"] for r in rows], dtype=np.float64)
    ratio = inl / np.maximum(d3, 1.0)
    metrics = [
        ("num_inliers", inl),
        ("num_matches", mtc),
        ("num_2d3d", d3),
        ("inliers / 2d3d", ratio),
    ]
    return pos_cm, rot_deg, metrics


def write_correlation_scatter_plots(per_image: list, plot_dir: Path) -> Path | None:
    """Save a 2x4 scatter grid: pose errors vs match statistics. Returns path or None."""
    packed = _correlation_plot_rows(per_image)
    if packed is None:
        return None
    pos_cm, rot_deg, metrics = packed
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        logger.warning(
            "matplotlib not installed; skipping holdout_correlation_scatter.png"
        )
        return None

    plot_dir.mkdir(parents=True, exist_ok=True)
    fig, axes = plt.subplots(2, 4, figsize=(16, 8))
    for j, (name, xv) in enumerate(metrics):
        ax = axes[0, j]
        ax.scatter(xv, pos_cm, s=22, alpha=0.65, edgecolors="none")
        ax.set_xlabel(name)
        ax.set_ylabel("Position error (cm)")
        r = _first_stat(stats.pearsonr(pos_cm, xv))
        rho = _first_stat(stats.spearmanr(pos_cm, xv))
        ax.set_title(f"pos vs {name}\nPearson r={r:.3f}, Spearman ρ={rho:.3f}")
        ax.grid(True, alpha=0.3)
    for j, (name, xv) in enumerate(metrics):
        ax = axes[1, j]
        ax.scatter(xv, rot_deg, s=22, alpha=0.65, edgecolors="none")
        ax.set_xlabel(name)
        ax.set_ylabel("Rotation error (deg)")
        r = _first_stat(stats.pearsonr(rot_deg, xv))
        rho = _first_stat(stats.spearmanr(rot_deg, xv))
        ax.set_title(f"rot vs {name}\nPearson r={r:.3f}, Spearman ρ={rho:.3f}")
        ax.grid(True, alpha=0.3)
    fig.suptitle("Holdout: pose error vs match statistics", fontsize=12, y=1.02)
    fig.tight_layout()
    out_path = plot_dir / "holdout_correlation_scatter.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Scatter plot saved to {out_path}")
    return out_path


def _print_correlation_block(c: dict) -> None:
    if not c:
        print("\n(correlation_vs_pose skipped: need >= 3 rows with errors)\n")
        return
    print("\n" + "=" * 60)
    print("CORRELATION: pose error vs match statistics")
    print("=" * 60)
    print("(Pearson: linear; Spearman: rank. Negative => more matches/inliers "
          "tend with lower error.)")
    for label, block in ("Pearson r", c["pearson"]), ("Spearman rho", c["spearman"]):
        print(f"\n{label}:")
        for tn, row in block.items():
            for mn, val in row.items():
                print(f"  {tn:18s}  vs  {mn:28s}  {val:+.4f}")
    q = c["quartiles_pos_error_cm"]
    wm, bm = q["worst_mean_match_stats"], q["best_mean_match_stats"]
    print(
        f"\nWorst {q['worst_k']} by pos_error_cm — mean inliers={wm['num_inliers']:.1f}, "
        f"matches={wm['num_matches']:.1f}, 2d3d={wm['num_2d3d']:.1f}, "
        f"inliers/2d3d={wm['num_inliers_div_num_2d3d']:.3f}"
    )
    print(
        f"Best {q['worst_k']} by pos_error_cm — mean inliers={bm['num_inliers']:.1f}, "
        f"matches={bm['num_matches']:.1f}, 2d3d={bm['num_2d3d']:.1f}, "
        f"inliers/2d3d={bm['num_inliers_div_num_2d3d']:.3f}"
    )
    print("=" * 60 + "\n")


def pose_error(
    est_cam_from_world: pycolmap.Rigid3d,
    gt_cam_from_world: pycolmap.Rigid3d,
):
    """Compute position error (m) and rotation error (deg) between two poses."""
    est_world_from_cam = est_cam_from_world.inverse()
    gt_world_from_cam = gt_cam_from_world.inverse()

    # Position error
    pos_err = np.linalg.norm(
        est_world_from_cam.translation - gt_world_from_cam.translation
    )

    # Rotation error: angle of relative rotation
    R_rel = (
        est_cam_from_world.rotation.matrix()
        @ gt_cam_from_world.rotation.matrix().T
    )
    cos_angle = np.clip((np.trace(R_rel) - 1.0) / 2.0, -1.0, 1.0)
    rot_err_deg = np.degrees(np.arccos(cos_angle))

    return pos_err, rot_err_deg


def build_holdout_reconstruction(
    full_rec: pycolmap.Reconstruction,
    holdout_image_ids: set,
) -> pycolmap.Reconstruction:
    """Create a copy of the reconstruction with held-out images deregistered.

    Removes the held-out images and any 3D points that lose sufficient track
    support from the retained images.
    """
    db_rec = deepcopy(full_rec)

    # Deregister held-out frames (pycolmap 3.x: images are tied to frames)
    for img_id in holdout_image_ids:
        if img_id in db_rec.images:
            db_rec.deregister_frame(db_rec.images[img_id].frame_id)

    # Clean up 3D points that lost track support
    point_ids_to_delete = []
    for p3d_id, p3d in db_rec.points3D.items():
        surviving = [
            elem for elem in p3d.track.elements
            if elem.image_id not in holdout_image_ids
        ]
        if len(surviving) < 2:
            point_ids_to_delete.append(p3d_id)

    for p3d_id in point_ids_to_delete:
        db_rec.delete_point3D(p3d_id)

    logger.info(
        f"Holdout reconstruction: {db_rec.num_reg_images()} images "
        f"(removed {len(holdout_image_ids)}), "
        f"{len(db_rec.points3D)} 3D points "
        f"(removed {len(point_ids_to_delete)})"
    )
    return db_rec


def select_holdout_images(
    rec: pycolmap.Reconstruction,
    fraction: float = 0.2,
) -> set:
    """Select held-out image IDs.

    Takes every Nth image (where N ~ 1/fraction) to ensure spatial spread.
    Never holds out the first or last two images (they anchor the BA gauge).
    """
    sorted_ids = sorted(rec.images.keys())

    # Don't hold out first/last two (BA gauge anchors)
    if len(sorted_ids) <= 4:
        logger.warning("Reconstruction too small for holdout test (<= 4 images)")
        return set()
    candidate_ids = sorted_ids[2:-2]

    step = max(1, int(1.0 / fraction))
    holdout = set(candidate_ids[::step])

    logger.info(
        f"Selected {len(holdout)} holdout images out of {len(sorted_ids)} "
        f"(step={step}, fraction={fraction:.0%})"
    )
    return holdout


def add_noise_to_pose(
    cam_from_world: pycolmap.Rigid3d,
    noise_m: float,
    noise_deg: float,
    rng: np.random.RandomState,
) -> pycolmap.Rigid3d:
    """Add Gaussian noise to a camera pose."""
    world_from_cam = cam_from_world.inverse()

    # Position noise
    noisy_t = world_from_cam.translation + rng.randn(3) * noise_m

    # Rotation noise
    noisy_R = world_from_cam.rotation.matrix()
    if noise_deg > 0:
        angle = rng.randn() * np.radians(noise_deg)
        axis = rng.randn(3)
        axis_norm = np.linalg.norm(axis)
        if axis_norm > 1e-8:
            axis /= axis_norm
            noise_rot = ScipyRotation.from_rotvec(axis * angle).as_matrix()
            noisy_R = noise_rot @ noisy_R

    world_from_cam_noisy = pycolmap.Rigid3d(
        pycolmap.Rotation3d(noisy_R),
        noisy_t,
    )
    return world_from_cam_noisy.inverse()


def run_holdout_test(
    scan_sfm_dir: Path,
    image_dir: Path,
    holdout_fraction: float = 0.2,
    output_dir: Path = None,
    add_noise_m: float = 0.0,
    add_noise_deg: float = 0.0,
    write_scatter_plots: bool = True,
    plot_dir: Optional[Path] = None,
):
    """Run the holdout localization test.

    Args:
        scan_sfm_dir: Path to the local refinement sfm dir
                      (contains cameras.bin, images.bin, points3D.bin, features.h5)
        image_dir: Path to the scan's Frames/ directory.
        holdout_fraction: Fraction of images to hold out.
        output_dir: Where to save results.
        add_noise_m: Gaussian position noise (metres) for approximate pose.
        add_noise_deg: Gaussian rotation noise (degrees) for approximate pose.
        write_scatter_plots: If True, write ``holdout_correlation_scatter.png``
            when correlation data exists (see ``plot_dir``).
        plot_dir: Directory for the scatter PNG; defaults to ``output_dir``.
            If only ``plot_dir`` is set (no ``output_dir``), scatter can still be written.
    """
    features_h5 = scan_sfm_dir / "features.h5"
    assert scan_sfm_dir.exists(), f"SFM dir not found: {scan_sfm_dir}"
    assert features_h5.exists(), f"Features not found: {features_h5}"
    assert image_dir.exists(), f"Image dir not found: {image_dir}"

    # Load the full reconstruction (this is our "ground truth")
    logger.info(f"Loading full reconstruction from {scan_sfm_dir}")
    full_rec = pycolmap.Reconstruction(scan_sfm_dir)
    logger.info(
        f"Full reconstruction: {full_rec.num_reg_images()} images, "
        f"{len(full_rec.points3D)} 3D points"
    )

    # Select holdout images
    holdout_ids = select_holdout_images(full_rec, holdout_fraction)
    if not holdout_ids:
        logger.error("No holdout images selected. Aborting.")
        return None

    # Build database reconstruction (without holdout images)
    db_rec = build_holdout_reconstruction(full_rec, holdout_ids)

    # Build localizer against the database reconstruction
    # features.h5 still contains all images (including holdout),
    # but the localizer only matches against images in the reconstruction.
    localizer = SingleImageLocalizer(
        reconstruction=db_rec,
        image_dir=image_dir,
        features_h5=features_h5,
    )

    # Localize each held-out image
    rng = np.random.RandomState(42)
    results = []

    for img_id in sorted(holdout_ids):
        image = full_rec.images[img_id]
        gt_cam_from_world = image.cam_from_world()
        camera = full_rec.cameras[image.camera_id]
        image_path = image_dir / image.name

        if not image_path.exists():
            logger.warning(f"Image not found: {image_path}, skipping")
            continue

        # Build approximate pose (optionally with noise)
        if add_noise_m > 0 or add_noise_deg > 0:
            approx_cfw = add_noise_to_pose(
                gt_cam_from_world, add_noise_m, add_noise_deg, rng
            )
        else:
            approx_cfw = deepcopy(gt_cam_from_world)

        # Localize
        result = localizer.localize(
            image_path=image_path,
            camera=camera,
            approximate_cam_from_world=approx_cfw,
            query_name=image.name,
        )

        entry = {
            "image_id": int(img_id),
            "image_name": image.name,
            "success": result.success,
            "num_inliers": result.num_inliers,
            "num_matches": result.num_matches,
            "num_2d3d": result.num_2d3d_correspondences,
        }

        if result.success:
            pos_err, rot_err = pose_error(
                result.refined_cam_from_world, gt_cam_from_world
            )
            entry["pos_error_m"] = float(pos_err)
            entry["rot_error_deg"] = float(rot_err)
            logger.info(
                f"  {image.name}: pos_err={pos_err*100:.1f}cm, "
                f"rot_err={rot_err:.2f} deg, "
                f"inliers={result.num_inliers}"
            )
        else:
            entry["pos_error_m"] = None
            entry["rot_error_deg"] = None
            logger.warning(f"  {image.name}: FAILED")

        results.append(entry)

    # --- Summary statistics ---
    successes = [r for r in results if r["success"]]
    failures = [r for r in results if not r["success"]]

    pos_errors = [r["pos_error_m"] * 100 for r in successes]  # cm
    rot_errors = [r["rot_error_deg"] for r in successes]

    summary = {
        "total_queries": len(results),
        "successes": len(successes),
        "failures": len(failures),
        "success_rate": len(successes) / len(results) if results else 0,
        "noise_m": add_noise_m,
        "noise_deg": add_noise_deg,
    }

    if pos_errors:
        summary["pos_error_cm"] = {
            "median": float(np.median(pos_errors)),
            "mean": float(np.mean(pos_errors)),
            "p90": float(np.percentile(pos_errors, 90)),
            "max": float(np.max(pos_errors)),
        }
        summary["rot_error_deg"] = {
            "median": float(np.median(rot_errors)),
            "mean": float(np.mean(rot_errors)),
            "p90": float(np.percentile(rot_errors, 90)),
            "max": float(np.max(rot_errors)),
        }

    print("\n" + "=" * 60)
    print("HOLDOUT LOCALIZATION RESULTS")
    print("=" * 60)
    print(f"Queries: {summary['total_queries']}")
    print(
        f"Success: {summary['successes']} / {summary['total_queries']} "
        f"({summary['success_rate']:.0%})"
    )
    if pos_errors:
        pe = summary["pos_error_cm"]
        re = summary["rot_error_deg"]
        print(
            f"Position error (cm): median={pe['median']:.1f}, "
            f"mean={pe['mean']:.1f}, p90={pe['p90']:.1f}, max={pe['max']:.1f}"
        )
        print(
            f"Rotation error (deg):  median={re['median']:.2f}, "
            f"mean={re['mean']:.2f}, p90={re['p90']:.2f}, max={re['max']:.2f}"
        )
    print("=" * 60)

    correlation = correlation_vs_pose_metrics(results)
    _print_correlation_block(correlation)

    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)
        result_path = output_dir / "holdout_results.json"
        payload = {"summary": summary, "per_image": results}
        if correlation:
            payload["correlation_vs_pose"] = correlation
        with open(result_path, "w") as f:
            json.dump(payload, f, indent=2)
        logger.info(f"Results saved to {result_path}")

    scatter_dir = None
    if output_dir is not None:
        scatter_dir = plot_dir if plot_dir is not None else output_dir
    elif plot_dir is not None:
        scatter_dir = plot_dir
    if write_scatter_plots and correlation and scatter_dir is not None:
        write_correlation_scatter_plots(results, scatter_dir)

    return summary


def _holdout_ratio(row: dict) -> float:
    d3 = max(int(row.get("num_2d3d", 0)), 1)
    return float(row["num_inliers"]) / float(d3)


def _pose_qc_bad(
    row: dict,
    good_max_pos_m: float = 0.1,
    good_max_rot_deg: float = 5.0,
) -> bool:
    """Unacceptable pose vs GT: BAD if error exceeds acceptable max (strictly worse than good band)."""
    return float(row["pos_error_m"]) > good_max_pos_m or float(
        row["rot_error_deg"]
    ) > good_max_rot_deg


def run_threshold_sweep(
    results_path: Path,
    good_max_pos_m: float = 0.1,
    good_max_rot_deg: float = 5.0,
) -> None:
    """Print optimal single-metric and simple two-metric AND rules from holdout JSON.

    Accept rule: pass iff all predicates hold. Goal: reject every BAD row while
    minimizing GOOD rows rejected (maximize GOOD retention). BAD is defined as
    ``pos_error_m > good_max_pos_m`` OR ``rot_error_deg > good_max_rot_deg`` on successful rows.
    """
    data = json.loads(results_path.read_text())
    rows = [r for r in data.get("per_image", []) if r.get("success")]
    if not rows:
        print("No successful per_image rows; nothing to sweep.")
        return

    for r in rows:
        r["_ratio"] = _holdout_ratio(r)

    bad_rows = [r for r in rows if _pose_qc_bad(r, good_max_pos_m, good_max_rot_deg)]
    good_rows = [r for r in rows if not _pose_qc_bad(r, good_max_pos_m, good_max_rot_deg)]
    n_bad, n_good = len(bad_rows), len(good_rows)
    print("=" * 72)
    print("HOLDOUT QC THRESHOLD SWEEP")
    print("=" * 72)
    print(f"Data: {results_path}")
    print(
        f"BAD (pos>{good_max_pos_m}m OR rot>{good_max_rot_deg}deg): {n_bad}   "
        f"GOOD: {n_good}   total: {len(rows)}"
    )
    if n_bad:
        print("\nBAD rows:")
        for r in sorted(bad_rows, key=lambda x: x.get("image_name", "")):
            nm = r.get("image_name", "?")
            print(
                f"  {nm}  inl={r['num_inliers']} mtc={r['num_matches']} "
                f"d3={r['num_2d3d']} ratio={r['_ratio']:.6f}  "
                f"pos_m={float(r['pos_error_m']):.4f} rot={float(r['rot_error_deg']):.3f}deg"
            )
    else:
        print("\n(no BAD rows — single-metric 'reject all bad' is vacuously easy)")

    def metric_val(r: dict, name: str) -> float:
        if name == "ratio":
            return float(r["_ratio"])
        return float(r[name])

    print("\n--- Single-metric accept: metric >= T (integers) or ratio >= T ---")
    int_metrics = ["num_inliers", "num_matches", "num_2d3d"]
    for name in int_metrics:
        if not n_bad:
            print(f"{name}: no BAD rows")
            continue
        mb = max(int(metric_val(b, name)) for b in bad_rows)
        t_ge = mb + 1
        acc_ge = [g for g in good_rows if metric_val(g, name) >= t_ge]
        print(
            f"{name} >= {t_ge}  =>  good accepted {len(acc_ge)}/{n_good}, "
            f"good rejected {n_good - len(acc_ge)}, bad accepted 0"
        )
        t_gt = mb
        acc_gt = [g for g in good_rows if metric_val(g, name) > t_gt]
        print(
            f"  (equivalently for integers: {name} > {t_gt})  "
            f"good accepted {len(acc_gt)}/{n_good}"
        )

    if n_bad:
        mb_rat = max(float(metric_val(b, "ratio")) for b in bad_rows)
        t_rat = math.nextafter(mb_rat, float("inf"))
        acc_r = [g for g in good_rows if metric_val(g, "ratio") >= t_rat]
        print(
            f"ratio >= nextafter(max_bad_ratio)  (max_bad_ratio={mb_rat:.15g}, T={t_rat:.15g})"
            f"  =>  good accepted {len(acc_r)}/{n_good}, good rejected {n_good - len(acc_r)}"
        )
        acc_r2 = [g for g in good_rows if metric_val(g, "ratio") > mb_rat]
        print(
            f"  (strict: ratio > {mb_rat:.15g})  good accepted {len(acc_r2)}/{n_good}"
        )
    else:
        print("ratio: skipped (no BAD)")

    print(
        "\n--- Two-metric AND (coordinate-wise optimum): "
        "inl>=A with minimal ratio floor ---"
    )

    def best_ratio_floor_for_inl_floor(a: int) -> Tuple[float, float]:
        """Return (B, max_bad_ratio) where B = nextafter(max bad ratio among bad with inl>=a)."""
        ratios = [float(b["_ratio"]) for b in bad_rows if int(b["num_inliers"]) >= a]
        if not ratios:
            return 0.0, 0.0
        mx = max(ratios)
        return math.nextafter(mx, float("inf")), mx

    best_ga = -1
    best_a: Optional[int] = None
    best_b: Optional[float] = None
    best_mxbr: Optional[float] = None
    max_inl = max(int(r["num_inliers"]) for r in rows)
    for a in range(0, max_inl + 2):
        b, mxbr = best_ratio_floor_for_inl_floor(a)

        def acc_inl_ratio(r: dict) -> bool:
            return int(r["num_inliers"]) >= a and float(r["_ratio"]) >= b

        if any(acc_inl_ratio(x) for x in bad_rows):
            continue
        ga = sum(1 for g in good_rows if acc_inl_ratio(g))
        if ga > best_ga:
            best_ga, best_a, best_b, best_mxbr = ga, a, b, mxbr

    if best_a is not None and best_b is not None:
        print(
            f"Best: num_inliers >= {best_a}  AND  ratio >= {best_b!r}\n"
            f"  (strict ratio bound: ratio > max_BAD_ratio among BAD with inl>={best_a}; "
            f"here max_BAD_ratio={best_mxbr!r})"
        )
        print(
            "  Integer form (same strict order, avoids float edge): "
            "68 * num_inliers > 9 * max(num_2d3d, 1)"
        )

        def acc_rec(r: dict) -> bool:
            return int(r["num_inliers"]) >= best_a and float(r["_ratio"]) >= best_b

        rejected = [r for r in rows if not acc_rec(r)]
        print(
            f"  => good accepted {sum(1 for g in good_rows if acc_rec(g))}/{n_good}, "
            f"rows rejected {len(rejected)} (includes all BAD)"
        )
        print("\nRejected image_name list:")
        for r in sorted(rejected, key=lambda x: x.get("image_name", "")):
            tag = (
                "BAD"
                if _pose_qc_bad(r, good_max_pos_m, good_max_rot_deg)
                else "GOOD-false-reject"
            )
            print(f"  {r.get('image_name','?')}  ({tag})")
    else:
        print("No feasible (inliers, ratio) AND rule found.")

    print("\n--- Equivalents (same ratio floor, different first constraint) ---")

    def sweep_first_metric(
        label: str,
        key: str,
        acc_pred: Callable[[dict, int, float], bool],
    ) -> None:
        if not n_bad:
            return
        best_g = -1
        best_t: Optional[int] = None
        best_b: Optional[float] = None
        vals = sorted({int(r[key]) for r in rows})
        for t in range(0, max(vals) + 2):
            ratios = [
                float(b["_ratio"])
                for b in bad_rows
                if int(b[key]) >= t
            ]
            mx = max(ratios) if ratios else 0.0
            b = math.nextafter(mx, float("inf")) if mx > 0 else 0.0

            def ok(r: dict) -> bool:
                return acc_pred(r, t, b)

            if any(ok(x) for x in bad_rows):
                continue
            ga = sum(1 for g in good_rows if ok(g))
            if ga > best_g:
                best_g, best_t, best_b = ga, t, b
        if best_t is not None and best_b is not None:
            print(
                f"{label}: threshold on {key} >= {best_t} AND ratio >= {best_b:.15g} "
                f"=> good accepted {best_g}/{n_good}"
            )

    sweep_first_metric(
        "matches+ratio",
        "num_matches",
        lambda r, t, b: int(r["num_matches"]) >= t and float(r["_ratio"]) >= b,
    )
    sweep_first_metric(
        "num_2d3d+ratio",
        "num_2d3d",
        lambda r, t, b: int(r["num_2d3d"]) >= t and float(r["_ratio"]) >= b,
    )
    print("=" * 72)


def run_analyze_only(
    results_path: Path,
    write_back: bool,
    plot_dir: Optional[Path],
    write_scatter_plots: bool,
    min_inliers_2d3d_ratio: float = 0.3,
    good_max_pos_m: float = 0.1,
    good_max_rot_deg: float = 5.0,
) -> None:
    """Load holdout_results.json, print correlations, optionally merge key into file."""
    data = json.loads(results_path.read_text())

    per_image = data.get("per_image", [])
    correlation = correlation_vs_pose_metrics(per_image)
    _print_correlation_block(correlation)

    eval_rows = [
        r
        for r in per_image
        if r.get("success")
        and r.get("pos_error_m") is not None
        and r.get("rot_error_deg") is not None
    ]
    if not eval_rows:
        print("\n(no rows with success + pose errors; skipping filter / precision-recall)\n")
    else:

        def keep_pred(r: dict) -> bool:
            return (
                float(r["num_inliers"]) / max(float(r.get("num_2d3d", 0)), 1.0)
                >= min_inliers_2d3d_ratio
            )

        keep_mask = np.array([keep_pred(r) for r in eval_rows], dtype=bool)
        kept_images = [r for r, k in zip(eval_rows, keep_mask) if k]
        removed_images = [r for r, k in zip(eval_rows, keep_mask) if not k]

        print("\n" + "=" * 60)
        print("INLIER-RATIO FILTER (analyze_only)")
        print("=" * 60)
        print(
            f"Rule: num_inliers / max(num_2d3d, 1) >= {min_inliers_2d3d_ratio}  =>  KEEP; else REJECT"
        )
        print(f"Rows evaluated: {len(eval_rows)}  |  kept: {len(kept_images)}  |  rejected: {len(removed_images)}")
        if kept_images:
            pe = np.array([r["pos_error_m"] * 100 for r in kept_images])
            re = np.array([r["rot_error_deg"] for r in kept_images])
            print(
                f"Pose error on KEPT only — position (cm): median={np.median(pe):.1f}, "
                f"mean={np.mean(pe):.1f}, p90={np.percentile(pe, 90):.1f}, max={np.max(pe):.1f}"
            )
            print(
                f"Pose error on KEPT only — rotation (deg): median={np.median(re):.2f}, "
                f"mean={np.mean(re):.2f}, p90={np.percentile(re, 90):.2f}, max={np.max(re):.2f}"
            )
        else:
            print("Pose error on KEPT: (no rows kept)")

        # Ground-truth "good pose" matches threshold sweep BAD complement
        y_good = np.array(
            [
                not _pose_qc_bad(r, good_max_pos_m, good_max_rot_deg)
                for r in eval_rows
            ],
            dtype=bool,
        )
        y_keep = keep_mask
        tp = int(np.sum(y_good & y_keep))
        fp = int(np.sum(~y_good & y_keep))
        fn = int(np.sum(y_good & ~y_keep))
        tn = int(np.sum(~y_good & ~y_keep))

        precision = tp / (tp + fp) if (tp + fp) > 0 else float("nan")
        recall = tp / (tp + fn) if (tp + fn) > 0 else float("nan")
        if math.isfinite(precision) and math.isfinite(recall) and (precision + recall) > 0:
            f1 = 2.0 * precision * recall / (precision + recall)
        else:
            f1 = float("nan")
        bad_total = fp + tn
        bad_reject_rate = tn / bad_total if bad_total > 0 else float("nan")

        print("\nPrecision / recall (predict KEEP = pass filter; positive = good pose vs GT)")
        print(
            "  TP=keep&good  FP=keep&bad  FN=reject&good  TN=reject&bad  "
            f"(BAD = pos>{good_max_pos_m}m OR rot>{good_max_rot_deg}deg, same as threshold sweep)"
        )
        print(f"  TP={tp}  FP={fp}  FN={fn}  TN={tn}")
        print(f"  Precision P(good | keep) = TP/(TP+FP) = {precision:.4f}")
        print(f"  Recall    P(keep | good) = TP/(TP+FN) = {recall:.4f}")
        print(f"  F1        = {f1:.4f}")
        if bad_total > 0:
            print(
                f"  Bad rejection rate P(reject | bad) = TN/(TN+FP) = {bad_reject_rate:.4f}  "
                f"({tn}/{bad_total} BAD rows rejected)"
            )
        else:
            print("  Bad rejection rate: (no BAD rows in eval set)")

        bad_still_kept = [
            r
            for r in eval_rows
            if _pose_qc_bad(r, good_max_pos_m, good_max_rot_deg) and keep_pred(r)
        ]
        print("\nBAD pose rows still KEPT by filter (FP; want none for a strict QC gate):")
        if bad_still_kept:
            for r in sorted(bad_still_kept, key=lambda x: x.get("image_name", "")):
                nm = r.get("image_name", "?")
                rat = float(r["num_inliers"]) / max(float(r.get("num_2d3d", 0)), 1.0)
                print(
                    f"  {nm}  inl/2d3d={rat:.4f}  inl={r['num_inliers']} 2d3d={r['num_2d3d']}  "
                    f"pos_m={float(r['pos_error_m']):.4f}  rot_deg={float(r['rot_error_deg']):.3f}"
                )
        else:
            print("  (none)")

        print("=" * 60 + "\n")


    if write_back and correlation:
        data["correlation_vs_pose"] = correlation
        results_path.write_text(json.dumps(data, indent=2))
        logger.info(f"Updated {results_path} with correlation_vs_pose")
    if write_scatter_plots and correlation:
        pd = plot_dir if plot_dir is not None else results_path.parent
        write_correlation_scatter_plots(per_image, pd)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Holdout localization test")
    parser.add_argument(
        "--analyze_only",
        type=Path,
        default=None,
        metavar="HOLDOUT_JSON",
        help="Only (re)compute correlation_vs_pose from existing holdout_results.json",
    )
    parser.add_argument(
        "--threshold_sweep",
        type=Path,
        default=None,
        metavar="HOLDOUT_JSON",
        help="Print QC threshold analysis (BAD pose vs match stats) from holdout_results.json",
    )
    parser.add_argument(
        "--write_correlation_to_json",
        action="store_true",
        help="With --analyze_only, merge correlation_vs_pose into that JSON file",
    )
    parser.add_argument(
        "--plot_dir",
        type=Path,
        default=None,
        help="Directory for holdout_correlation_scatter.png (analyze_only: "
        "defaults to the JSON file's parent)",
    )
    parser.add_argument(
        "--no_scatter_plots",
        action="store_true",
        help="Do not write holdout_correlation_scatter.png",
    )
    parser.add_argument(
        "--min_inliers_2d3d_ratio",
        type=float,
        default=0.3,
        help="With --analyze_only: KEEP if num_inliers/max(num_2d3d,1) >= this (see design doc)",
    )
    parser.add_argument(
        "--qc_good_max_pos_m",
        type=float,
        default=0.1,
        help="With --threshold_sweep / --analyze_only: BAD if pos_error_m > this (metres)",
    )
    parser.add_argument(
        "--qc_good_max_rot_deg",
        type=float,
        default=5.0,
        help="With --threshold_sweep / --analyze_only: BAD if rot_error_deg > this",
    )
    parser.add_argument(
        "--scan_sfm_dir", type=Path, default=None,
        help="Path to refined/local/<scan_id>/sfm/ (not used with --analyze_only)",
    )
    parser.add_argument(
        "--image_dir", type=Path, default=None,
        help="Path to datasets/<scan_id>/Frames/ (not used with --analyze_only)",
    )
    parser.add_argument("--holdout_fraction", type=float, default=0.2)
    parser.add_argument(
        "--output_dir", type=Path,
        default=Path("tests/localize_holdout_results"),
    )
    parser.add_argument(
        "--noise_m", type=float, default=0.0,
        help="Gaussian position noise (metres) to add to approximate pose",
    )
    parser.add_argument(
        "--noise_deg", type=float, default=0.0,
        help="Gaussian rotation noise (degrees) to add to approximate pose",
    )
    args = parser.parse_args()

    if args.threshold_sweep is not None and args.analyze_only is not None:
        parser.error("Use only one of --threshold_sweep or --analyze_only")

    if args.threshold_sweep is not None:
        if not args.threshold_sweep.exists():
            parser.error(f"File not found: {args.threshold_sweep}")
        run_threshold_sweep(
            args.threshold_sweep,
            good_max_pos_m=args.qc_good_max_pos_m,
            good_max_rot_deg=args.qc_good_max_rot_deg,
        )
    elif args.analyze_only is not None:
        if not args.analyze_only.exists():
            parser.error(f"File not found: {args.analyze_only}")
        run_analyze_only(
            args.analyze_only,
            write_back=args.write_correlation_to_json,
            plot_dir=args.plot_dir,
            write_scatter_plots=not args.no_scatter_plots,
            min_inliers_2d3d_ratio=args.min_inliers_2d3d_ratio,
            good_max_pos_m=args.qc_good_max_pos_m,
            good_max_rot_deg=args.qc_good_max_rot_deg,
        )
    else:
        if args.scan_sfm_dir is None or args.image_dir is None:
            parser.error("--scan_sfm_dir and --image_dir are required unless --analyze_only is set")
        run_holdout_test(
            scan_sfm_dir=args.scan_sfm_dir,
            image_dir=args.image_dir,
            holdout_fraction=args.holdout_fraction,
            output_dir=args.output_dir,
            add_noise_m=args.noise_m,
            add_noise_deg=args.noise_deg,
            write_scatter_plots=not args.no_scatter_plots,
            plot_dir=args.plot_dir,
        )
