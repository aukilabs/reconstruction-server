#pragma once

#include <limits>

#include <Eigen/Core>
#include <ceres/ceres.h>
#include <ceres/conditioned_cost_function.h>
#include <ceres/normal_prior.h>
#include <ceres/rotation.h>
#include <sophus/se3.hpp>
#include <sophus/sim3.hpp>

namespace auki {

template <typename T>
using EigenQuaternionMap = Eigen::Map<const Eigen::Quaternion<T>>;
template <typename T>
using EigenVector3Map = Eigen::Map<const Eigen::Matrix<T, 3, 1>>;

class DistanceMovedCostFunction {
 public:
  explicit DistanceMovedCostFunction(
      const Eigen::Vector4d& arkit_offset_rotated,
      const Eigen::Vector3d& arkit_offset_moved,
      const Eigen::Vector3d& arkit_gravity_direction,
      const double offset_weight,
      const double gravity_weight)
      : arkit_offset_rotated_(arkit_offset_rotated),
        arkit_offset_moved_(arkit_offset_moved),
        arkit_gravity_direction_(arkit_gravity_direction),
        arkit_offset_(Eigen::Quaterniond(arkit_offset_rotated.data()),
                      arkit_offset_moved),
        offset_weight_(offset_weight),
        gravity_weight_(gravity_weight) {}

  static ceres::CostFunction* Create(
      const Eigen::Vector4d& arkit_offset_rotated,
      const Eigen::Vector3d& arkit_offset_moved,
      const Eigen::Vector3d& arkit_gravity_direction,
      const double offset_weight,
      const double gravity_weight) {
    return (new ceres::
                AutoDiffCostFunction<DistanceMovedCostFunction, 1, 4, 3, 4, 3>(
                    new DistanceMovedCostFunction(arkit_offset_rotated,
                                                  arkit_offset_moved,
                                                  arkit_gravity_direction,
                                                  offset_weight,
                                                  gravity_weight)));
  }

  template <typename T>
  bool operator()(const T* const cam_from_world_rotation,
                  const T* const cam_from_world_translation,
                  const T* const prev_cam_from_world_rotation,
                  const T* const prev_cam_from_world_translation,
                  T* residuals) const {
    const Sophus::SE3<T> cam_from_world =
        Sophus::SE3<T>(EigenQuaternionMap<T>(cam_from_world_rotation),
                       Eigen::Matrix<T, 3, 1>(cam_from_world_translation));
    const Sophus::SE3<T> prev_world_from_cam =
        Sophus::SE3<T>(EigenQuaternionMap<T>(prev_cam_from_world_rotation),
                       Eigen::Matrix<T, 3, 1>(prev_cam_from_world_translation))
            .inverse();

    const Sophus::SE3<T> offset = cam_from_world * prev_world_from_cam;

    // const Eigen::Quaternion<T> prev_world_from_cam_rotation =
    //     EigenQuaternionMap<T>(prev_cam_from_world_rotation).inverse();
    // const Eigen::Matrix<T, 3, 1> prev_world_from_cam_translation =
    //     prev_world_from_cam_rotation *
    //     -EigenVector3Map<T>(prev_cam_from_world_translation);

    // const Eigen::Quaternion<T> offset_rotation =
    //     (EigenQuaternionMap<T>(cam_from_world_rotation) *
    //      prev_world_from_cam_rotation)
    //         .normalized();
    // const Eigen::Matrix<T, 3, 1> offset_translation =
    //     EigenVector3Map<T>(cam_from_world_translation) +
    //     (EigenQuaternionMap<T>(cam_from_world_rotation) *
    //      prev_world_from_cam_translation);

    T position_cost =
        T(offset_weight_) *
        (arkit_offset_moved_.cast<T>() - offset.translation()).norm();

    if (position_cost < std::numeric_limits<T>::epsilon()) {
      position_cost = T(0.0);
    }

    T angle_cost = T(offset_weight_) * offset.unit_quaternion().angularDistance(
                                           arkit_offset_rotated_.cast<T>());

    if (angle_cost < std::numeric_limits<T>::epsilon()) {
      angle_cost = T(0.0);
    }

    // const Eigen::Matrix<T, 3, 1> gravity_direction =
    //     EigenQuaternionMap<T>(cam_from_world_rotation) *
    //         Eigen::Matrix<T, 3, 1>(T(-1.0), T(0.0), T(0.0)) +
    //     EigenVector3Map<T>(cam_from_world_translation);
    // T gravity_cost =
    //     T(gravity_weight_) *
    //     ceres::acos(arkit_gravity_direction_.normalized().cast<T>().dot(
    //         gravity_direction.normalized())) *
    //     T(180.0 / M_PI);
    // if (gravity_cost < std::numeric_limits<T>::epsilon()) {
    //     gravity_cost = T(0.0);
    // }

    // residuals[0] = position_cost + angle_cost + gravity_cost;
    residuals[0] = position_cost + angle_cost;
    return true;
  }

 private:
  const Eigen::Quaterniond arkit_offset_rotated_;
  const Eigen::Vector3d arkit_offset_moved_;
  const Eigen::Vector3d arkit_gravity_direction_;
  const Sophus::SE3d arkit_offset_;
  const double offset_weight_;
  const double gravity_weight_;
};

class RelativeTransformationSE3CostFunction {
 public:
  using SE3 = Sophus::SE3<double>;
  static constexpr int residuals_num = SE3::DoF;

  RelativeTransformationSE3CostFunction(
      const Eigen::Vector4d& t_target_reference_quat,
      const Eigen::Vector3d& t_target_reference_translation,
      const Eigen::Matrix<double, SE3::DoF, SE3::DoF>&
          t_reference_target_covariance)
      : t_target_reference_(Eigen::Quaterniond(t_target_reference_quat.data()),
                            t_target_reference_translation),
        normal_prior_(new ceres::NormalPrior(
            t_reference_target_covariance.inverse().llt().matrixU(),
            Eigen::Matrix<double, residuals_num, 1>::Zero())) {}

  static ceres::CostFunction* Create(
      const Eigen::Vector4d& t_target_reference_quat,
      const Eigen::Vector3d& t_target_reference_translation,
      const Eigen::Matrix<double, SE3::DoF, SE3::DoF>&
          t_reference_target_covariance) {
    return (
        new ceres::AutoDiffCostFunction<RelativeTransformationSE3CostFunction,
                                        residuals_num,
                                        4,
                                        3,
                                        4,
                                        3>(
            new RelativeTransformationSE3CostFunction(
                t_target_reference_quat,
                t_target_reference_translation,
                t_reference_target_covariance)));
  }

  template <typename T>
  bool operator()(const T* const t_target_local_quat,
                  const T* const t_target_local_translation, // pose
                  const T* const t_reference_local_quat,
                  const T* const t_reference_local_translation, // prev_pose
                  T* residuals) const {
    const Sophus::SE3<T> t_target_local =
        Sophus::SE3<T>(EigenQuaternionMap<T>(t_target_local_quat),
                       Eigen::Matrix<T, 3, 1>(t_target_local_translation));
    const Sophus::SE3<T> t_reference_local =
        Sophus::SE3<T>(EigenQuaternionMap<T>(t_reference_local_quat),
                       Eigen::Matrix<T, 3, 1>(t_reference_local_translation));

    Eigen::Matrix<T, residuals_num, 1> parameters =
        (
            t_target_reference_.cast<T>() * // Expected relative transform (from constructor)
            (t_target_local.inverse() * t_reference_local) // Actual relative transform (from current parameters)
        ).log();

    return normal_prior_(parameters.data(), residuals);
  }

 private:
  const Sophus::SE3d t_target_reference_;
  const ceres::CostFunctionToFunctor<residuals_num, residuals_num>
      normal_prior_;
};

class RelativeTransformationSE3ViaObservationsCostFunction {
 public:
  using SE3 = RelativeTransformationSE3CostFunction::SE3;
  static constexpr int residuals_num =
      RelativeTransformationSE3CostFunction::residuals_num;

  RelativeTransformationSE3ViaObservationsCostFunction(
      const Eigen::Vector4d& t_reference_observation_quat,
      const Eigen::Vector3d& t_reference_observation_translation,
      const Eigen::Vector4d& t_target_observation_quat,
      const Eigen::Vector3d& t_target_observation_translation,
      const Eigen::Matrix<double, SE3::DoF, SE3::DoF>&
          t_reference_target_covariance)
      : t_reference_observation_(
            Eigen::Quaterniond(t_reference_observation_quat.data()),
            t_reference_observation_translation),
        t_target_observation_(
            Eigen::Quaterniond(t_target_observation_quat.data()),
            t_target_observation_translation),
        normal_prior_(new ceres::NormalPrior(
            t_reference_target_covariance.inverse().llt().matrixU(),
            Eigen::Matrix<double, residuals_num, 1>::Zero())) {}

  static ceres::CostFunction* Create(
      const Eigen::Vector4d& t_reference_observation_quat,
      const Eigen::Vector3d& t_reference_observation_translation,
      const Eigen::Vector4d& t_target_observation_quat,
      const Eigen::Vector3d& t_target_observation_translation,
      const Eigen::Matrix<double, SE3::DoF, SE3::DoF>&
          t_reference_target_covariance) {
    return (new ceres::AutoDiffCostFunction<
            RelativeTransformationSE3ViaObservationsCostFunction,
            residuals_num,
            4,
            3,
            4,
            3>(new RelativeTransformationSE3ViaObservationsCostFunction(
        t_reference_observation_quat,
        t_reference_observation_translation,
        t_target_observation_quat,
        t_target_observation_translation,
        t_reference_target_covariance)));
  }

  template <typename T>
  bool operator()(const T* const t_target_local_quat,
                  const T* const t_target_local_translation,
                  const T* const t_reference_local_quat,
                  const T* const t_reference_local_translation,
                  T* residuals) const {
    const Sophus::SE3<T> t_target_local =
        Sophus::SE3<T>(EigenQuaternionMap<T>(t_target_local_quat),
                       Eigen::Matrix<T, 3, 1>(t_target_local_translation));
    const Sophus::SE3<T> t_reference_local =
        Sophus::SE3<T>(EigenQuaternionMap<T>(t_reference_local_quat),
                       Eigen::Matrix<T, 3, 1>(t_reference_local_translation));

    const Sophus::SE3<T> t_local_observation_reference =
        t_reference_local.inverse() * t_reference_observation_;
    const Sophus::SE3<T> t_local_observation_target =
        t_target_local.inverse() * t_target_observation_;

    const Sophus::SE3<T> t_observation_target_observation_reference =
        t_local_observation_target.inverse() * t_local_observation_reference;

    const Eigen::Matrix<T, residuals_num, 1> parameters =
        t_observation_target_observation_reference.log();

    return normal_prior_(parameters.data(), residuals);
  }

 private:
  const Sophus::SE3d t_reference_observation_;
  const Sophus::SE3d t_target_observation_;
  const ceres::CostFunctionToFunctor<residuals_num, residuals_num>
      normal_prior_;
};

class RelativeTransformationSim3CostFunction {
 public:
  using Sim3 = Sophus::Sim3<double>;
  using SE3 = Sophus::SE3<double>;
  static constexpr int residuals_num = SE3::DoF;

  RelativeTransformationSim3CostFunction(
      const Eigen::Vector4d& t_reference_observation_quat,
      const Eigen::Vector3d& t_reference_observation_translation,
      const Eigen::Vector4d& t_target_observation_quat,
      const Eigen::Vector3d& t_target_observation_translation,
      const Eigen::Matrix<double, residuals_num, residuals_num>&
          t_reference_target_covariance)
      : t_reference_observation_(
            Eigen::Quaterniond(t_reference_observation_quat.data()),
            t_reference_observation_translation),
        t_target_observation_(
            Eigen::Quaterniond(t_target_observation_quat.data()),
            t_target_observation_translation),
        normal_prior_(new ceres::NormalPrior(
            t_reference_target_covariance.inverse().llt().matrixU(),
            Eigen::Matrix<double, residuals_num, 1>::Zero())) {}

  static ceres::CostFunction* Create(
      const Eigen::Vector4d& t_reference_observation_quat,
      const Eigen::Vector3d& t_reference_observation_translation,
      const Eigen::Vector4d& t_target_observation_quat,
      const Eigen::Vector3d& t_target_observation_translation,
      const Eigen::Matrix<double, residuals_num, residuals_num>&
          t_reference_target_covariance) {
    return (
        new ceres::AutoDiffCostFunction<RelativeTransformationSim3CostFunction,
                                        residuals_num,
                                        4,
                                        3,
                                        4,
                                        3>(
            new RelativeTransformationSim3CostFunction(
                t_reference_observation_quat,
                t_reference_observation_translation,
                t_target_observation_quat,
                t_target_observation_translation,
                t_reference_target_covariance)));
  }

  template <typename T>
  bool operator()(const T* const t_local_target_quat,
                  const T* const t_local_target_translation,
                  const T* const t_local_reference_quat,
                  const T* const t_local_reference_translation,
                  T* residuals) const {
    const Sophus::Sim3<T> t_local_target =
        Sophus::Sim3<T>(EigenQuaternionMap<T>(t_local_target_quat),
                        Eigen::Matrix<T, 3, 1>(t_local_target_translation));
    const Sophus::Sim3<T> t_local_reference =
        Sophus::Sim3<T>(EigenQuaternionMap<T>(t_local_reference_quat),
                        Eigen::Matrix<T, 3, 1>(t_local_reference_translation));

    const Sophus::Sim3<T> t_local_target_observation =
        t_local_target * t_target_observation_.cast<T>();
    const Sophus::Sim3<T> t_local_reference_observation =
        t_local_reference * t_reference_observation_.cast<T>();

    const Sophus::Sim3<T> t_observation_target_observation_reference =
        t_local_target_observation.inverse() * t_local_reference_observation;

    const Sophus::SE3<T> t_observation_target_observation_reference_new =
        Sophus::SE3<T>(
            t_observation_target_observation_reference.quaternion()
                .normalized(),
            t_observation_target_observation_reference.translation() *
                t_observation_target_observation_reference.scale());

    const Eigen::Matrix<T, residuals_num, 1> parameters =
        t_observation_target_observation_reference_new.log();

    return normal_prior_(parameters.data(), residuals);
  }

 private:
  const Sophus::Sim3d t_reference_observation_;
  const Sophus::Sim3d t_target_observation_;
  const ceres::CostFunctionToFunctor<residuals_num, residuals_num>
      normal_prior_;
};

class GravityDirectionPriorCostFunction {
 public:
  GravityDirectionPriorCostFunction(const Eigen::Vector3d& local_gravity_direction, const double weight)
      : local_gravity_direction_(local_gravity_direction.normalized()), 
        weight_(weight),
        world_gravity_direction_(Eigen::Vector3d(-1.0, 0.0, 0.0)) {}

  static ceres::CostFunction* Create(const Eigen::Vector3d& local_gravity_direction, const double weight) {
    return new ceres::AutoDiffCostFunction<GravityDirectionPriorCostFunction, 3, 4>(
        new GravityDirectionPriorCostFunction(local_gravity_direction, weight));
  }

  template <typename T>
  bool operator()(const T* const local_from_world_rotation, T* residuals) const {

    const Eigen::Matrix<T, 3, 1> current_local_gravity_direction =
        EigenQuaternionMap<T>(local_from_world_rotation) *
        world_gravity_direction_.cast<T>();
    
    // Vector difference as residual (x,y,z components)
    Eigen::Map<Eigen::Matrix<T, 3, 1>> residuals_eigen(residuals);
    residuals_eigen = T(weight_) * (current_local_gravity_direction - local_gravity_direction_.cast<T>());

    return true;
  }

 private:
  Eigen::Vector3d local_gravity_direction_;  // Normalized in constructor
  Eigen::Vector3d world_gravity_direction_;  // Defined as (-1,0,0) for COLMAP coordinate system
  double weight_;
};

class PoseCenterConstraintCostFunction {
 public:
  PoseCenterConstraintCostFunction(const Eigen::Vector3d& center,
                                   const Eigen::Vector3d& weight)
      : weight_(weight), pose_center_constraint_(center) {}

  static ceres::CostFunction* Create(
      const Eigen::Vector3d& pose_center_constraint,
      const Eigen::Vector3d& weight) {
    return (new ceres::
                AutoDiffCostFunction<PoseCenterConstraintCostFunction, 3, 4, 3>(
                    new PoseCenterConstraintCostFunction(pose_center_constraint,
                                                         weight)));
  }

  template <typename T>
  bool operator()(const T* const cam_from_world_rotation,
                  const T* const cam_from_world_translation,
                  T* residuals) const {
    const Eigen::Matrix<T, 3, 1> pose_center =
        EigenQuaternionMap<T>(cam_from_world_rotation).inverse() *
        -EigenVector3Map<T>(cam_from_world_translation);

    Eigen::Map<Eigen::Matrix<T, 3, 1>> residuals_eigen(residuals);
    residuals_eigen = weight_.cast<T>().cwiseProduct(
        pose_center - pose_center_constraint_.cast<T>());

    return true;
  }

 private:
  Eigen::Vector3d weight_;
  Eigen::Vector3d pose_center_constraint_;
};

class FloorAlignmentCostFunction {
 public:
  FloorAlignmentCostFunction(
      const Eigen::Vector4d& detection_rotation,
      const Eigen::Vector3d& detection_translation,
      const double height_weight = 1.0,
      const double direction_weight = 1.0)
      : detection_(Eigen::Quaterniond(detection_rotation.data()),
                  detection_translation),
        height_weight_(height_weight),
        direction_weight_(direction_weight) {}

  static ceres::CostFunction* Create(
      const Eigen::Vector4d& detection_rotation,
      const Eigen::Vector3d& detection_translation,
      const double height_weight = 1.0,
      const double direction_weight = 1.0) {
    return new ceres::AutoDiffCostFunction<FloorAlignmentCostFunction, 2, 4, 3>(
        new FloorAlignmentCostFunction(detection_rotation, detection_translation,
                                     height_weight, direction_weight));
  }

  template <typename T>
  bool operator()(const T* const cam_from_world_rotation,
                 const T* const cam_from_world_translation,
                 T* residuals) const {

    // Create SE3 object and store it
    const Sophus::SE3<T> cam_from_world = Sophus::SE3<T>(
        EigenQuaternionMap<T>(cam_from_world_rotation),
        Eigen::Matrix<T, 3, 1>(cam_from_world_translation));

    // Transform detection from camera to world space
    const Sophus::SE3<T> world_from_qr = 
        cam_from_world.inverse() * detection_.cast<T>();

    // Height error - in COLMAP space, x is up, so constrain x=0
    residuals[0] = T(height_weight_) * world_from_qr.translation().x();

    // Direction error - local z axis in world space should point towards (-1,0,0)
    const Eigen::Matrix<T, 3, 1> local_z_in_world = 
        world_from_qr.rotationMatrix() * Eigen::Matrix<T, 3, 1>(T(0), T(0), T(1));
    
    // Should align with (-1,0,0) in COLMAP world space
    const Eigen::Matrix<T, 3, 1> target_direction(T(-1), T(0), T(0));
    residuals[1] = T(direction_weight_) * 
        (T(1) - local_z_in_world.dot(target_direction));

    return true;
  }

 private:
  const Sophus::SE3d detection_;  // QR detection in camera space
  const double height_weight_;
  const double direction_weight_;
};

}  // namespace auki