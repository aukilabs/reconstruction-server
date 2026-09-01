#include "feature/ALIKED.hpp"
#include "matcher/lightglue/matcher.hpp"

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <opencv2/opencv.hpp>
#include <torch/torch.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace py = pybind11;

namespace {

py::array_t<float> tensor_to_numpy_f32(torch::Tensor tensor) {
    tensor = tensor.detach().cpu().contiguous().to(torch::kFloat32);
    const auto sizes = tensor.sizes();
    std::vector<ssize_t> shape(sizes.begin(), sizes.end());
    py::array_t<float> array(shape);
    std::memcpy(array.mutable_data(), tensor.data_ptr<float>(),
                static_cast<size_t>(tensor.numel()) * sizeof(float));
    return array;
}

py::dict feats_to_py_dict(const torch::Dict<std::string, torch::Tensor>& feats,
                          const std::vector<std::string>& keys) {
    py::dict out;
    for (const auto& key : keys) {
        if (!feats.contains(key)) {
            continue;
        }
        out[py::str(key)] = tensor_to_numpy_f32(feats.at(key));
    }
    return out;
}

torch::Tensor numpy_f32_to_tensor(const py::array& array, const torch::Device& device) {
    py::array_t<float, py::array::c_style | py::array::forcecast> arr(array);
    auto info = arr.request();
    if (info.ndim == 0) {
        throw std::runtime_error("expected ndarray with at least one dimension");
    }
    std::vector<int64_t> shape;
    for (ssize_t i = 0; i < info.ndim; ++i) {
        shape.push_back(static_cast<int64_t>(info.shape[i]));
    }
    auto tensor = torch::from_blob(info.ptr, shape, torch::kFloat32).clone();
    return tensor.to(device);
}

torch::Dict<std::string, torch::Tensor> py_feats_to_torch(const py::dict& feats,
                                                          const torch::Device& device) {
    torch::Dict<std::string, torch::Tensor> out;
    for (auto item : feats) {
        const auto key = py::cast<std::string>(item.first);
        const auto value = py::cast<py::array>(item.second);
        out.insert(key, numpy_f32_to_tensor(value, device));
    }
    return out;
}

cv::Mat numpy_rgb_to_mat(const py::array& array) {
    py::array_t<uint8_t, py::array::c_style | py::array::forcecast> arr(array);
    auto info = arr.request();
    if (info.ndim != 3 || info.shape[2] != 3) {
        throw std::runtime_error("extract_from_numpy expects HxWx3 uint8 RGB array");
    }
    const int rows = static_cast<int>(info.shape[0]);
    const int cols = static_cast<int>(info.shape[1]);
    cv::Mat mat(rows, cols, CV_8UC3, info.ptr);
    return mat.clone();
}

cv::Mat load_rgb_image(const std::string& path) {
    cv::Mat bgr = cv::imread(path);
    if (bgr.empty()) {
        throw std::runtime_error("failed to load image: " + path);
    }
    cv::Mat rgb;
    cv::cvtColor(bgr, rgb, cv::COLOR_BGR2RGB);
    return rgb;
}

} // namespace

class PyAlikedExtractor {
public:
    // top_k / n_limit match ALIKED C++ ctor; default top_k=1024 mirrors hloc aliked-n16.
    PyAlikedExtractor(const std::string& model_name, const std::string& device,
                      int top_k, float scores_th, int nms_radius, int n_limit,
                      int resize_max)
        : resize_max_(resize_max),
          device_(torch::Device(device)),
          extractor_(std::make_shared<ALIKED>(model_name, device, top_k, scores_th,
                                              n_limit, nms_radius)) {}

    py::dict extract_from_path(const std::string& path) {
        cv::Mat rgb = load_rgb_image(path);
        return extract_from_mat(rgb);
    }

    py::dict extract_from_numpy(const py::array& array) {
        cv::Mat rgb = numpy_rgb_to_mat(array);
        return extract_from_mat(rgb);
    }

private:
    py::dict extract_from_mat(cv::Mat& rgb) {
        const int orig_w = rgb.cols;
        const int orig_h = rgb.rows;

        cv::Mat process_img = rgb;
        float scale_x = 1.0f;
        float scale_y = 1.0f;

        if (resize_max_ > 0) {
            const int max_side = std::max(orig_w, orig_h);
            if (max_side > resize_max_) {
                const float scale =
                    static_cast<float>(resize_max_) / static_cast<float>(max_side);
                const int new_w =
                    static_cast<int>(std::lround(static_cast<float>(orig_w) * scale));
                const int new_h =
                    static_cast<int>(std::lround(static_cast<float>(orig_h) * scale));
                cv::resize(rgb, process_img, cv::Size(new_w, new_h), 0, 0,
                           cv::INTER_AREA);
                scale_x = static_cast<float>(orig_w) / static_cast<float>(new_w);
                scale_y = static_cast<float>(orig_h) / static_cast<float>(new_h);
            }
        }

        auto feats = extractor_->run(process_img);

        if (scale_x != 1.0f || scale_y != 1.0f) {
            auto kpts = feats.at("keypoints");
            const auto scales =
                torch::tensor({scale_x, scale_y},
                              torch::TensorOptions().dtype(kpts.dtype()).device(kpts.device()));
            kpts = (kpts + 0.5) * scales - 0.5;
            TORCH_CHECK(feats.erase("keypoints"),
                        "Failed to remove 'keypoints' from output dict");
            feats.insert("keypoints", kpts);
        }

        const float width = static_cast<float>(orig_w);
        const float height = static_cast<float>(orig_h);
        feats.insert(
            "image_size",
            torch::tensor({width, height}, torch::TensorOptions().dtype(torch::kFloat32))
                .unsqueeze(0));

        static const std::vector<std::string> keys = {
            "keypoints", "descriptors", "scores", "image_size"};
        return feats_to_py_dict(feats, keys);
    }

    int resize_max_;
    torch::Device device_;
    std::shared_ptr<ALIKED> extractor_;
};

class PyLightGlueMatcher {
public:
    explicit PyLightGlueMatcher(const std::string& device)
        : device_(torch::Device(device)),
          matcher_(std::make_shared<matcher::LightGlue>()) {
        matcher_->to(device_);
    }

    py::dict match(const py::dict& feats0, const py::dict& feats1) {
        auto data0 = py_feats_to_torch(feats0, device_);
        auto data1 = py_feats_to_torch(feats1, device_);
        auto matches = matcher_->forward(data0, data1);

        static const std::vector<std::string> keys = {
            "matches0", "matching_scores0", "matches1", "matching_scores1"};
        return feats_to_py_dict(matches, keys);
    }

private:
    torch::Device device_;
    std::shared_ptr<matcher::LightGlue> matcher_;
};

PYBIND11_MODULE(mrnerf_features, m) {
    m.doc() = "MrNeRF ALIKED extract + LightGlue match (LibTorch C++)";

    m.def("cuda_available", []() { return torch::cuda::is_available(); });

    py::class_<PyAlikedExtractor>(m, "AlikedExtractor")
        .def(py::init<const std::string&, const std::string&, int, float, int, int, int>(),
             py::arg("model_name") = "aliked-n16",
             py::arg("device") = "cuda",
             py::arg("top_k") = 1024,
             py::arg("scores_th") = 0.3f,
             py::arg("nms_radius") = 4,
             py::arg("n_limit") = 20000,
             py::arg("resize_max") = 1024)
        .def("extract_from_path", &PyAlikedExtractor::extract_from_path,
             py::arg("path"))
        .def("extract_from_numpy", &PyAlikedExtractor::extract_from_numpy,
             py::arg("image_rgb"));

    py::class_<PyLightGlueMatcher>(m, "LightGlueMatcher")
        .def(py::init<const std::string&>(), py::arg("device") = "cuda")
        .def("match", &PyLightGlueMatcher::match, py::arg("features0"),
             py::arg("features1"));
}
