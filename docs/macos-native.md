# MacOS native build (experimental)

This document describes an **experimental** path for running the reconstruction compute stack on **Apple Silicon** with **native** Python (venv on the host), not Linux Docker.

**Why not Docker on Mac?** PyTorch **MPS** (Metal) is only available in macOS/arm64 wheels. Linux containers on Mac do not expose MPS the same way; for practical throughput on Apple Silicon you use a local venv. The supported production path for NVIDIA GPUs remains [Deployment](deployment.md) (Docker + CUDA).

## What you need to know

- **hloc fork:** Feature extraction, matching, and `utils/triangulation.py` assume **`hloc.utils.inference_device`** (CUDA / MPS / CPU selection and `HLOC_DEVICE`). That module is **not** in upstream CVG hloc; use the **Auki Labs fork** on branch **`auki-master`** (`https://github.com/aukilabs/Hierarchical-Localization`), which includes `inference_device` and the MPS-oriented fixes this repo expects. Install from git as in the quick start below, or `pip install -e /path/to/Hierarchical-Localization` from a local checkout of that branch.
- **C++ and Rust:** You still build this repo’s native code and the Rust `compute-node` on the host; see [Development Guide](development.md) for CMake layout and Rust toolchain expectations.
- **COLMAP / Ceres / glog:** Typical flow is Homebrew for libraries, then build COLMAP from source with **CUDA off**, install **pycolmap** / **pyceres** into the same venv as PyTorch and hloc.

## Quick start (Python deps)

From the **reconstruction-server** repository root. Use **default PyPI** on **macosx_arm64** for PyTorch (MPS); do **not** point `pip` at Linux CUDA wheel indexes (`download.pytorch.org` `cu*`, and so on).

```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip install -U pip setuptools wheel

# PyTorch (default PyPI on macosx_arm64 includes MPS)
pip install "torch>=2.1" "torchvision>=0.16" "torchaudio>=2.1"

pip install \
  "numpy>=1.24" "scipy>=1.10" "opencv-python-headless>=4.8" \
  "PyYAML>=6.0" "python-dateutil>=2.8" "psutil>=5.9" "GPUtil>=1.4" \
  "h5py>=3.8" "tqdm>=4.65" \
  "trimesh>=4.0" "alphashape>=1.3" "enlighten>=1.10" \
  "scikit-learn>=1.3" "matplotlib>=3.7" "shapely>=2.0" \
  "pybind11[global]>=3.0.1"

pip install "pycolmap>=3.11.0"

pip install "pyceres @ git+https://github.com/cvg/pyceres.git@v2.5"
pip install "hloc @ git+https://github.com/aukilabs/Hierarchical-Localization.git@auki-master"
```

If you maintain a local hloc clone instead of the last line, use `pip install -e /path/to/Hierarchical-Localization` (same `auki-master`-based tree).

## Device selection

- Default order: CUDA (if available) → MPS (Apple) → CPU.
- Override: `HLOC_DEVICE=cpu`, `mps`, or `cuda` (case-insensitive).

Matching in `utils/triangulation.py` uses **CPU** for the LightGlue step when the resolved device is **MPS** (workaround for slow or fragile MPS matching); feature extraction may still use MPS unless you force `HLOC_DEVICE=cpu`.

## OpenMP (`OMP: Error #15` / SIGABRT)

`pycolmap` and `torch` can each link a different OpenMP runtime on macOS. If **torch** loads **after** native code that already initialized OpenMP, the process may abort.

`main.py` imports **torch before** other entry-point imports **on Darwin only**, which avoids the common bad ordering.

Last resort (unsafe; can hide real bugs): `export KMP_DUPLICATE_LIB_OK=TRUE`.

## Native stack outline (manual)

High-level steps used in internal validation (adjust versions to your environment):

1. **Toolchain:** Xcode CLT, Homebrew, `cmake`, `ninja`, Rust (see `rust-toolchain` / server README if pinned).
2. **glog:** build/install from source if needed by Ceres/COLMAP.
3. **Ceres:** Eigen (e.g. `brew install eigen`), build Ceres from source, install.
4. **pyceres:** `pip install "pyceres @ git+https://github.com/cvg/pyceres.git@v2.5"` (same tag as Docker / Linux docs in this repo).
5. **COLMAP:** clone, checkout a revision compatible with your **pycolmap** wheel or source install, `cmake` with `-DCUDA_ENABLED=OFF` (and GUI off if you prefer), build, `pip install -e .` for pycolmap in the same venv.
6. **Draco / compression:** Prefer **Homebrew** or a clone **outside** this repository (do not commit a full `draco/` tree into `reconstruction-server`).
7. **This repo:** `cmake -B build -DCMAKE_BUILD_TYPE=Release` (see [Development](development.md)), build Python extensions; `cargo build --release` under `server/rust`, copy `compute-node` to the repo root as documented.

If CMake fails finding OpenGL on Mac when COLMAP expects it, you may need a small CMake guard in your COLMAP tree—treat that as **local troubleshooting**, not something the open-source Dockerfile path relies on.

## Smoke tests

From **reconstruction-server** root with the same venv you use for jobs:

```bash
python tests/test_pyceres_smoke.py
python tests/test_pycolmap_smoke.py
python tests/test_hloc_smoke.py
```

Optional **feature/match** smoke (two images from a directory; committed fixtures under `tests/data/test_frames/`):

```bash
python tests/test_hloc_feature_match_dmt_smoke.py --frames-dir tests/data/test_frames
# optional: --plots-dir /tmp/hloc_plots  (or HLOC_SMOKE_PLOT_DIR) — one PNG per consecutive frame pair
```

## See also

- [Development Guide](development.md) — dev container (Linux + NVIDIA) vs native Mac.
- [Minimum Requirements](minimum-requirements.md) — supported production hardware; Mac is supplemental.
