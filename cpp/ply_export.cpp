#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <colmap/util/ply.h>
#include <colmap/scene/reconstruction.h>

namespace py = pybind11;

void ExportPLYText(const colmap::Reconstruction& reconstruction, const std::string& path) {
    const auto ply_points = reconstruction.ConvertToPLY();
    const bool kWriteNormal = false;
    const bool kWriteRGB = true;
    colmap::WriteTextPlyPoints(path, ply_points, kWriteNormal, kWriteRGB);
}

PYBIND11_MODULE(ply_export, m) {
    m.def("export_ply_text", &ExportPLYText, "Export reconstruction to PLY text file",
          py::arg("reconstruction"), py::arg("path"));
}