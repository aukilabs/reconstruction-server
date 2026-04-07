#include <algorithm>
#include <unordered_map>
#include <vector>

#include <pybind11/pybind11.h>

#include <colmap/scene/camera.h>
#include <colmap/scene/frame.h>
#include <colmap/scene/image.h>
#include <colmap/scene/point2d.h>
#include <colmap/scene/point3d.h>
#include <colmap/scene/reconstruction.h>
#include <colmap/scene/track.h>
#include <colmap/sensor/rig.h>
#include <colmap/util/types.h>

namespace py = pybind11;

namespace {

colmap::camera_t NextSharedObjectId(const colmap::Reconstruction& reconstruction) {
  colmap::camera_t next_id = 1;
  for (const auto& [camera_id, _] : reconstruction.Cameras()) {
    next_id = std::max(next_id, camera_id + 1);
  }
  for (const auto& [rig_id, _] : reconstruction.Rigs()) {
    next_id = std::max(next_id, rig_id + 1);
  }
  for (const auto& [frame_id, _] : reconstruction.Frames()) {
    next_id = std::max(next_id, frame_id + 1);
  }
  for (const auto& [image_id, _] : reconstruction.Images()) {
    next_id = std::max(next_id, image_id + 1);
  }
  return next_id;
}

void AppendReconstruction(colmap::Reconstruction& destination,
                          const colmap::Reconstruction& source) {
  colmap::camera_t next_id = NextSharedObjectId(destination);
  std::unordered_map<colmap::image_t, colmap::image_t> image_id_old_to_new;

  std::vector<colmap::image_t> reg_image_ids = source.RegImageIds();
  std::sort(reg_image_ids.begin(), reg_image_ids.end());

  for (const colmap::image_t old_image_id : reg_image_ids) {
    const colmap::Image& old_image = source.Image(old_image_id);
    const colmap::Camera& old_camera = source.Camera(old_image.CameraId());
    const colmap::Frame& old_frame = source.Frame(old_image.FrameId());
    const colmap::Rig& old_rig = source.Rig(old_frame.RigId());

    const colmap::camera_t new_id = next_id++;
    const colmap::sensor_t new_sensor(colmap::SensorType::CAMERA, new_id);

    colmap::Camera new_camera = old_camera;
    new_camera.camera_id = new_id;
    destination.AddCamera(std::move(new_camera));

    colmap::Rig new_rig;
    new_rig.SetRigId(new_id);
    if (old_rig.RefSensorId().type == colmap::SensorType::CAMERA) {
      new_rig.AddRefSensor(new_sensor);
    } else {
      new_rig.AddSensor(new_sensor);
    }
    destination.AddRig(std::move(new_rig));

    colmap::Frame new_frame = old_frame;
    new_frame.ResetRigPtr();
    new_frame.SetRigId(new_id);
    new_frame.SetFrameId(new_id);
    new_frame.DataIds().clear();
    new_frame.AddDataId(colmap::data_t(new_sensor, new_id));
    destination.AddFrame(std::move(new_frame));

    colmap::Image new_image = old_image;
    new_image.ResetCameraPtr();
    new_image.ResetFramePtr();
    new_image.SetImageId(new_id);
    new_image.SetCameraId(new_id);
    new_image.SetFrameId(new_id);
    for (colmap::point2D_t point2D_idx = 0; point2D_idx < new_image.NumPoints2D();
         ++point2D_idx) {
      if (new_image.Point2D(point2D_idx).HasPoint3D()) {
        new_image.ResetPoint3DForPoint2D(point2D_idx);
      }
    }
    destination.AddImage(std::move(new_image));
    destination.RegisterFrame(new_id);

    image_id_old_to_new.emplace(old_image_id, new_id);
  }

  for (const auto& [_, old_point3D] : source.Points3D()) {
    colmap::Track new_track;
    new_track.Reserve(old_point3D.track.Length());
    for (const colmap::TrackElement& old_track_element :
         old_point3D.track.Elements()) {
      const auto it = image_id_old_to_new.find(old_track_element.image_id);
      if (it == image_id_old_to_new.end()) {
        continue;
      }
      new_track.AddElement(it->second, old_track_element.point2D_idx);
    }

    if (new_track.Length() == 0) {
      continue;
    }

    destination.AddPoint3D(old_point3D.xyz, std::move(new_track), old_point3D.color);
  }
}

}  // namespace

PYBIND11_MODULE(reconstruction_merge, m) {
  py::module::import("pycolmap");

  m.def("append_reconstruction",
        [](colmap::Reconstruction& destination,
           const colmap::Reconstruction& source) {
          py::gil_scoped_release release;
          AppendReconstruction(destination, source);
        },
        py::arg("destination"),
        py::arg("source"),
        "Append a transformed reconstruction into a combined reconstruction.");
}
