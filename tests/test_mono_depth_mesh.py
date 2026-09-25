import logging
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from utils.mono_depth_mesh import (
    DEFAULT_PROCESS_RES,
    DEFAULT_STRIDE,
    _stage_fused_depths,
    ensure_colmap_dataset_layout,
    global_mesh_ply_path,
    local_scan_depth_dir,
    maybe_run_global_mono_depth_mesh,
    maybe_run_mono_depth_mesh,
    mono_depth_mesh_enabled,
    mono_depth_mesh_enabled_from_env,
    run_global_mono_depth_mesh,
    run_mono_depth_mesh,
)


class MonoDepthMeshFlagTests(unittest.TestCase):
    def test_default_stride_is_three(self):
        self.assertEqual(DEFAULT_STRIDE, 3)

    def test_env_off_by_default(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertFalse(mono_depth_mesh_enabled_from_env())

    def test_env_on(self):
        with mock.patch.dict(os.environ, {"MONO_DEPTH_MESH": "1"}):
            self.assertTrue(mono_depth_mesh_enabled())

    def test_cli_overrides_without_env(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertTrue(mono_depth_mesh_enabled(cli_flag=True))


class MaybeRunMonoDepthMeshTests(unittest.TestCase):
    def test_flag_off_does_not_call_run_mesh(self):
        with mock.patch("utils.mono_depth_mesh.run_mono_depth_mesh") as run_mock:
            maybe_run_mono_depth_mesh(
                False,
                Path("/tmp/out"),
                Path("/tmp/images"),
                Path("/tmp/sparse"),
            )
            run_mock.assert_not_called()


class RunMonoDepthMeshSoftFailTests(unittest.TestCase):
    def test_run_mesh_exception_soft_fails(self):
        log = logging.getLogger("test_mono_depth_mesh")
        fake_run_mesh = mock.Mock(side_effect=RuntimeError("gpu"))
        pipeline_mod = mock.Mock(run_mesh=fake_run_mesh)
        with mock.patch.dict(
            "sys.modules",
            {"colmap_monodepth.pipeline": pipeline_mod},
        ):
            with mock.patch(
                "utils.mono_depth_mesh.ensure_colmap_dataset_layout",
                return_value=Path("/tmp/out"),
            ):
                result = run_mono_depth_mesh(
                    Path("/tmp/out"),
                    Path("/tmp/images"),
                    Path("/tmp/sparse"),
                    log=log,
                )
        self.assertIsNone(result)

    def test_run_mesh_called_with_defaults(self):
        out = Path("/tmp/scan_out")
        images = Path("/tmp/frames")
        sparse = Path("/tmp/sfm")
        fake_run_mesh = mock.Mock()
        pipeline_mod = mock.Mock(run_mesh=fake_run_mesh)
        with mock.patch.dict(
            "sys.modules",
            {"colmap_monodepth.pipeline": pipeline_mod},
        ):
            with mock.patch(
                "utils.mono_depth_mesh.ensure_colmap_dataset_layout",
                return_value=out,
            ) as layout_mock:
                with mock.patch.object(Path, "is_file", return_value=True):
                    run_mono_depth_mesh(out, images, sparse)
        layout_mock.assert_called_once_with(out, images, sparse)
        fake_run_mesh.assert_called_once_with(
            out,
            out,
            stride=DEFAULT_STRIDE,
            process_res=DEFAULT_PROCESS_RES,
            device="cuda",
        )


class EnsureColmapLayoutTests(unittest.TestCase):
    def test_creates_symlinks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scan_out = root / "scan"
            images = root / "Frames"
            sparse = root / "sfm"
            scan_out.mkdir()
            images.mkdir()
            sparse.mkdir()

            colmap_dir = ensure_colmap_dataset_layout(scan_out, images, sparse)
            self.assertEqual(colmap_dir, scan_out)
            images_link = scan_out / "images"
            sparse_link = scan_out / "sparse"
            self.assertTrue(images_link.exists())
            self.assertTrue(sparse_link.exists())
            self.assertEqual(images_link.resolve(), images.resolve())
            self.assertEqual(sparse_link.resolve(), sparse.resolve())


class LocalScanDepthDirTests(unittest.TestCase):
    def test_prefers_carve_over_confident(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            carve = root / "carve"
            confident = root / "fit" / "confident"
            carve.mkdir(parents=True)
            confident.mkdir(parents=True)
            (carve / "a_depth.png").write_bytes(b"x")
            (confident / "b_depth.png").write_bytes(b"y")
            self.assertEqual(local_scan_depth_dir(root), carve)

    def test_falls_back_to_confident(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            confident = root / "fit" / "confident"
            confident.mkdir(parents=True)
            (confident / "b_depth.png").write_bytes(b"y")
            self.assertEqual(local_scan_depth_dir(root), confident)


class GlobalMonoDepthMeshTests(unittest.TestCase):
    def test_flag_off_does_not_fuse(self):
        with mock.patch("utils.mono_depth_mesh.run_global_mono_depth_mesh") as run_mock:
            maybe_run_global_mono_depth_mesh(
                False,
                Path("/job"),
                Path("/out"),
                ["scan_a"],
                {},
            )
            run_mock.assert_not_called()

    def _fake_numpy_module(self):
        fake_np = mock.MagicMock()
        fake_np.concatenate = lambda parts, axis=0: parts[0]
        fake_np.asarray = lambda values, dtype=None: values
        return fake_np

    def _collect_return_one_view(self):
        return (
            ["scan_a__frame.jpg"],
            mock.Mock(),
            mock.Mock(),
            [640],
            [480],
        )

    def test_global_fuse_soft_fails(self):
        log = logging.getLogger("test_global_mono_depth_mesh")
        fake_tsdf_mod = mock.Mock(run_tsdf=mock.Mock(side_effect=RuntimeError("tsdf")))
        fake_types_mod = mock.Mock(FrameSet=mock.Mock(return_value=mock.Mock()))
        with mock.patch(
            "utils.mono_depth_mesh._collect_aligned_scan_views",
            return_value=self._collect_return_one_view(),
        ):
            with mock.patch(
                "utils.mono_depth_mesh.local_scan_depth_dir",
                return_value=Path("/depth"),
            ):
                with mock.patch("utils.mono_depth_mesh._stage_fused_depths"):
                    with mock.patch.dict(
                        "sys.modules",
                        {
                            "numpy": self._fake_numpy_module(),
                            "colmap_monodepth.tsdf": fake_tsdf_mod,
                            "colmap_monodepth.types": fake_types_mod,
                        },
                    ):
                        result = run_global_mono_depth_mesh(
                            Path("/job"),
                            Path("/out"),
                            ["scan_a"],
                            {"scan_a": mock.Mock()},
                            log=log,
                        )
        self.assertIsNone(result)

    def _fake_types_module(self):
        return mock.Mock(
            FrameSet=mock.Mock(return_value=mock.Mock()),
            TsdfConfig=lambda **kwargs: SimpleNamespace(**kwargs),
        )

    def test_global_fuse_calls_tsdf(self):
        out = Path("/out/global")
        fake_tsdf_mod = mock.Mock(
            run_tsdf=mock.Mock(return_value=mock.Mock(mesh_path=global_mesh_ply_path(out)))
        )
        fake_types_mod = self._fake_types_module()
        with mock.patch(
            "utils.mono_depth_mesh._collect_aligned_scan_views",
            return_value=self._collect_return_one_view(),
        ):
            with mock.patch(
                "utils.mono_depth_mesh.local_scan_depth_dir",
                return_value=Path("/carve"),
            ):
                with mock.patch("utils.mono_depth_mesh._stage_fused_depths"):
                    with mock.patch("utils.topology_export.promote_tsdf_ply_to_topology"):
                        with mock.patch.object(Path, "is_file", return_value=True):
                            with mock.patch.dict(
                                "sys.modules",
                                {
                                    "numpy": self._fake_numpy_module(),
                                    "colmap_monodepth.tsdf": fake_tsdf_mod,
                                    "colmap_monodepth.types": fake_types_mod,
                                },
                            ):
                                run_global_mono_depth_mesh(
                                    Path("/job"),
                                    out,
                                    ["scan_a"],
                                    {"scan_a": mock.Mock()},
                                )
        fake_tsdf_mod.run_tsdf.assert_called_once()
        call_args = fake_tsdf_mod.run_tsdf.call_args
        self.assertEqual(call_args[0][1], out / "mesh")
        config = call_args.kwargs.get("config")
        self.assertIsNotNone(config)
        self.assertEqual(getattr(config, "color_type", None), "nocolor")

    def test_global_fuse_passes_alignment_scale_to_staging(self):
        out = Path("/out/global")
        alignment = mock.Mock()
        alignment.scale = 2.5
        fake_tsdf_mod = mock.Mock(
            run_tsdf=mock.Mock(return_value=mock.Mock(mesh_path=global_mesh_ply_path(out)))
        )
        fake_types_mod = self._fake_types_module()
        with mock.patch(
            "utils.mono_depth_mesh._collect_aligned_scan_views",
            return_value=self._collect_return_one_view(),
        ):
            with mock.patch(
                "utils.mono_depth_mesh.local_scan_depth_dir",
                return_value=Path("/carve"),
            ):
                with mock.patch("utils.mono_depth_mesh._stage_fused_depths") as stage_mock:
                    with mock.patch("utils.topology_export.promote_tsdf_ply_to_topology"):
                        with mock.patch.object(Path, "is_file", return_value=True):
                            with mock.patch.dict(
                                "sys.modules",
                                {
                                    "numpy": self._fake_numpy_module(),
                                    "colmap_monodepth.tsdf": fake_tsdf_mod,
                                    "colmap_monodepth.types": fake_types_mod,
                                },
                            ):
                                run_global_mono_depth_mesh(
                                    Path("/job"),
                                    out,
                                    ["scan_a"],
                                    {"scan_a": alignment},
                                )
        stage_mock.assert_called_once()
        self.assertEqual(stage_mock.call_args[0][3], 2.5)


class StageFusedDepthsScaleTests(unittest.TestCase):
    @staticmethod
    def _fake_depth_io_module():
        depth_io = mock.MagicMock()

        def _depth_png_path(depth_dir, image_name):
            return Path(depth_dir) / f"{Path(image_name).stem}_depth.png"

        depth_io.depth_png_path.side_effect = _depth_png_path
        pkg = mock.MagicMock()
        pkg.depth_io = depth_io
        return pkg, depth_io

    def test_staging_multiplies_depth_by_alignment_scale(self):
        pkg, depth_io = self._fake_depth_io_module()
        src_depth = [[1.0, 2.0], [0.5, 0.0]]

        class _DepthArray:
            def __init__(self, rows):
                self._rows = [list(r) for r in rows]

            def __mul__(self, other):
                return _DepthArray(
                    [[v * other for v in row] for row in self._rows]
                )

            def tolist(self):
                return self._rows

        depth_io.load_depth_png.return_value = _DepthArray(src_depth)
        saved = []

        def _save_depth_png(depth_m, path):
            saved.append((depth_m.tolist(), Path(path)))
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).write_bytes(b"png")
            return Path(path)

        depth_io.save_depth_png.side_effect = _save_depth_png

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "carve"
            staging = root / "staging"
            source.mkdir()
            (source / "frame_depth.png").write_bytes(b"src")

            with mock.patch.dict(
                "sys.modules",
                {"colmap_monodepth": pkg, "colmap_monodepth.depth_io": depth_io},
            ):
                _stage_fused_depths(
                    ["scan_a__frame.jpg"],
                    source,
                    staging,
                    depth_scale=2.0,
                )

            depth_io.load_depth_png.assert_called_once_with(source / "frame_depth.png")
            self.assertEqual(len(saved), 1)
            self.assertEqual(saved[0][0], [[2.0, 4.0], [1.0, 0.0]])
            self.assertEqual(saved[0][1], staging / "scan_a__frame_depth.png")

    def test_staging_scale_one_copies_without_decode(self):
        pkg, depth_io = self._fake_depth_io_module()

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "carve"
            staging = root / "staging"
            source.mkdir()
            src_file = source / "frame_depth.png"
            src_file.write_bytes(b"original")

            with mock.patch.dict(
                "sys.modules",
                {"colmap_monodepth": pkg, "colmap_monodepth.depth_io": depth_io},
            ):
                _stage_fused_depths(
                    ["scan_a__frame.jpg"],
                    source,
                    staging,
                    depth_scale=1.0,
                )

            depth_io.load_depth_png.assert_not_called()
            depth_io.save_depth_png.assert_not_called()
            self.assertEqual(
                (staging / "scan_a__frame_depth.png").read_bytes(),
                b"original",
            )


if __name__ == "__main__":
    unittest.main()
