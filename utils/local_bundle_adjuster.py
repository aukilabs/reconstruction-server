"""
Python reimplementation of the bundle adjustment for the incremental mapper of C++ with equivalent logic.
As a result, one can add customized residuals on top of the exposed ceres problem from conventional bundle adjustment.
pyceres is needed as a dependency for this file.
"""

import pyceres
import pycolmap
from numpy.linalg import norm
import numpy as np

from utils.data_utils import vec3_angle, pycolmap_to_batch_matrix
# from utils_OLD import *

# Get all detections of same QR code as close to each other as possible.
class CustomLoopClosureCostFunction(pyceres.CostFunction):
    def __init__(self, detections_per_image_id, qr_world_points_per_image_id=None, weight=1.0, const_positions={}, const_quaternions={}, debugging=False):
    #def __init__(self, detections_per_image_id, weight=1.0, const_positions={}, const_quaternions={}, debugging=False):
        # MUST BE CALLED. Initializes the Ceres::CostFunction class
        super().__init__()

        # MUST BE CALLED. Sets the size of the residuals and parameters
        self.set_num_residuals(1)

        parameter_block_sizes = []
        for i, image_id in enumerate(detections_per_image_id.keys()):
            if image_id not in const_positions.keys():
                parameter_block_sizes.append(3) # cam_from_world translation
            if image_id not in const_quaternions.keys():
                parameter_block_sizes.append(4) # cam_from_world rotation

        self.set_parameter_block_sizes(parameter_block_sizes)

        self.detections_per_image_id = detections_per_image_id
        self.qr_world_points_per_image_id = qr_world_points_per_image_id
        self.const_positions = const_positions
        self.const_quaternions = const_quaternions
        self.weight = weight
        self.debugging = debugging
        self.debug_print_iteration = 0

    def Evaluate(self, parameters, residuals, jacobians):

        num_cameras = len(parameters) // 2
        if (num_cameras <= 1):
            print("Loop Closure loss not valid for <= 1 detection. DEBUG: len(parameters) was", len(parameters))
            residuals[0] = 0
            return False

        def calc_loss(params, debug_print=False):
            world_positions = []
            world_up_vecs = []
            world_right_vecs = []

            i = 0
            mean_height_diffs = []
            for image_id in self.detections_per_image_id.keys():

                if image_id in self.const_positions.keys():
                    translation = self.const_positions[image_id]
                else:
                    translation = params[i][0:3]
                    i += 1

                if image_id in self.const_quaternions.keys():
                    quaternion = self.const_quaternions[image_id]
                else:
                    quaternion = params[i][0:4]
                    i += 1

                cam_from_world = pycolmap.Rigid3d(pycolmap.Rotation3d(quaternion), translation)

                for detection in self.detections_per_image_id[image_id]:
                    world_pose = cam_from_world.inverse() * detection
                    world_positions.append(world_pose.translation)
                    world_up = np.array(world_pose.rotation * ([1.0, 0.0, 0.0]))
                    world_up_vecs.append(world_up)
                    world_right_vecs.append(np.array(world_pose.rotation * ([0.0, 1.0, 0.0])))

                    # Loop through 3D points which are part of the QR code
                    # We want all of those to be "on" the QR code in world space. Not above or below.
                    # This ensures the refinement keeps the right scale.
                    # Only looking at visuals gives us a scale-invariant output but since we know
                    # the physical size of each QR code, we also know how far away from the camera it is.
                    # If the 3D reconstruction gets too small or big, the floor and all detected 3D points
                    # would be at the wrong depth.
                    if self.qr_world_points_per_image_id is not None:
                        height_diffs = []
                        for qr_point in self.qr_world_points_per_image_id[image_id]:
                            # Check how far above or below the QR code "plane" each feature point is.
                            # If it's perfectly "on" the plane it would become 0.

                            offset_from_point = qr_point.xyz - world_pose.translation
                            height_diff = offset_from_point.dot(world_up)
                            height_diff = np.abs(height_diff) # important! positive and negative must not cancel out.
                            height_diffs.append(height_diff)

                        if len(height_diffs) > 0:
                            height_diffs.sort()
                            cutoff = len(height_diffs) // 4 # Cut worst 25% to remove outliers
                            if cutoff > 0:
                                height_diffs = height_diffs[:-cutoff]
                            # In reality the feature points are a bit noisy so we want a "best fit"
                            mean_height_diffs.append(np.mean(height_diffs))

            pos_deviations = np.std(world_positions, axis=0)
            pos_deviation = np.mean(pos_deviations)
            up_deviations = np.std(world_up_vecs, axis=0)
            up_deviation = np.mean(up_deviations)
            right_deviations = np.std(world_right_vecs, axis=0)
            right_deviation = np.mean(right_deviations)


            # Make sure the 3D reconstructed floor stays at the same distance to the cam as when the app detected it.
            # Otherwise the reconstructed world can grow or shrink.
            # This works since we know the physical size of portals.
            qr_distance_loss = np.mean(mean_height_diffs) if len(mean_height_diffs) > 0 else 0.0

            if debug_print:
                print(f"Loop Closure deviation {pos_deviation:.6f} from {len(world_positions)} detections.",
                      f"Const pos [{','.join(str(id) for id in self.const_positions.keys())}].",
                      f"Const rot [{','.join(str(id) for id in self.const_quaternions.keys())}].",
                      f"qr_distance_loss={qr_distance_loss:.6f},",
                      f"mean_height_diffs={mean_height_diffs}"
                      f"up_deviations={up_deviations}, right_deviations={right_deviations},"
                      f"pos_deviations={pos_deviations}, world_positions={world_positions}")


            # TODO move this to separate residual:


            #return (pos_deviation + up_deviation + right_deviation) * self.weight * 10000
            return pos_deviation * self.weight * 10000 + qr_distance_loss * 1000

        debug_print = self.debugging and self.debug_print_iteration % 25 == 0

        residuals[0] = calc_loss(parameters, debug_print)

        self.debug_print_iteration += 1

        if jacobians is not None:  # check for Null
            for parameter_block_index in range(len(jacobians)):
                for parameter_index in range(len(jacobians[parameter_block_index])):
                    # Evaluates the cost function again but with a small increase on one parameter. Then dy/dx to approximate slope.
                    new_params = [block.copy() for block in parameters]
                    dx = 0.00001
                    new_params[parameter_block_index][parameter_index] += dx
                    new_residual = calc_loss(new_params)
                    dy = new_residual - residuals[0]
                    jacobians[parameter_block_index][parameter_index] = dy / dx

        if debug_print:
            print("Jacobians:", jacobians)

        return True

# From https://github.com/Edwinem/ceres_python_bindings
class DistanceMovedCostFunction(pyceres.CostFunction):

    def __init__(self, arkit_offset_moved, arkit_offset_rotated, arkit_gravity_direction,
                 const_prev_pos, const_prev_quat,
                 offset_weight,
                 gravity_weight,
                 image_id_debug,
                 debugging=False):
        # MUST BE CALLED. Initializes the Ceres::CostFunction class
        super().__init__()

        # MUST BE CALLED. Sets the size of the residuals and parameters
        self.set_num_residuals(1)
        param_block_sizes = [3,4] # cam position, cam quaternion
        if const_prev_pos is None:
            param_block_sizes.append(3) # prev cam position
        if const_prev_quat is None:
            param_block_sizes.append(4) # prev cam quaternion

        self.set_parameter_block_sizes(param_block_sizes)

        self.arkit_offset_moved = arkit_offset_moved
        self.arkit_offset_rotated = arkit_offset_rotated

        self.offset_weight = offset_weight
        self.gravity_weight = gravity_weight
        self.debug_print_iteration = 0
        self.debugging = debugging
        self.image_id_debug = image_id_debug
        self.arkit_gravity_direction = arkit_gravity_direction  # NOTE: the gravity transformed into the camera space. Not just 0,1,0.
        self.const_prev_pos = const_prev_pos
        self.const_prev_quat = const_prev_quat


    # The CostFunction::Evaluate(...) virtual function implementation
    def Evaluate(self, parameters, residuals, jacobians):
        def calc_loss(params, debug_print=False):

            pos = params[0][0:3]
            quat = params[1][0:4]

            #i = 2
            #if self.const_prev_pos is not None:
            #    prev_pos = self.const_prev_pos
            #else:
            #    prev_pos = params[i][0:3]
            #    i += 1

            #if self.const_prev_quat is not None:
            #    prev_quat = self.const_prev_quat
            #else:
            #    prev_quat = params[i][0:4]
            #    i += 1

            cam_from_world = pycolmap.Rigid3d(pycolmap.Rotation3d(quat), pos)
            #prev_cam_from_world = pycolmap.Rigid3d(pycolmap.Rotation3d(prev_quat), prev_pos)

            #offset = cam_from_world * prev_cam_from_world.inverse()

            #position_cost = self.offset_weight * norm(self.arkit_offset_moved - offset.translation)

            #angle_offset = offset.rotation.angle_to(self.arkit_offset_rotated)
            #angle_cost = self.offset_weight * angle_offset * 5

            # Trust the gravity more from ARKit. That is already pretty accurate.
            gravity_direction = np.matmul(cam_from_world.matrix(), np.array([-1.0, 0.0, 0.0, 0.0]).transpose())[:3]
            gravity_cost = self.gravity_weight * vec3_angle(self.arkit_gravity_direction, gravity_direction)

            if debug_print:
                print(f"Iteration {self.debug_print_iteration} gravity cost: {gravity_cost}")# (position: {position_cost} + angle: {angle_cost} + gravity: {gravity_cost})  :::  offset moved: {offset.translation}, arkit offset: {self.arkit_offset_moved}")

            return gravity_cost
            #return position_cost + angle_cost + gravity_cost

        debug_print = self.debugging and self.debug_print_iteration % 25 == 0
        loss = calc_loss(parameters, debug_print=debug_print)
        if debug_print:
            print(f"Iteration {self.debug_print_iteration} image {self.image_id_debug}: loss = {loss}")

        residuals[0] = loss

        self.debug_print_iteration += 1

        # Just try to make it work for now with numerical approximation of each partial derivative
        # TODO: Future speedup with analytic jacobian and c++
        if jacobians is not None:  # check for Null
            for parameter_block_index in range(len(jacobians)):
                for parameter_index in range(len(jacobians[parameter_block_index])):
                    # Evaluates the cost function again but with a small increase on one parameter. Then dy/dx to approximate slope.
                    new_params = [block.copy() for block in parameters]
                    dx = 0.0001
                    new_params[parameter_block_index][parameter_index] += dx
                    new_residual = calc_loss(new_params)
                    dy = new_residual - residuals[0]
                    jacobians[parameter_block_index][parameter_index] = dy / dx
                    #if debug_print:
                    #    print(f"J[{parameter_block_index}][{parameter_index}]: dy={dy:.6f}, dx={dy:.6f}, dy/dx={dy/dx:.6f},",
                    #          f"new_residual={new_residual}, old_residual={residuals[0]}")

        if debug_print:
            print("Jacobians:", jacobians)


        return True


class PyBundleAdjuster(object):
    # Python implementation of COLMAP bundle adjuster with pyceres
    def __init__(
        self,
        options: pycolmap.BundleAdjustmentOptions,
        config: pycolmap.BundleAdjustmentConfig,
        refinement_config={}
    ):
        self.options = options
        self.config = config
        self.refinement_config = refinement_config
        self.problem = pyceres.Problem()
        self.summary = pyceres.SolverSummary()
        self.camera_ids = set()
        self.point3D_num_observations = dict()

    def set_up_problem(
        self,
        detections_per_qr,
        image_ids_per_qr,
        timestamps_per_image,
        arkit_precomputed,
        reconstruction: pycolmap.Reconstruction,
        loss: pyceres.LossFunction,
    ):
        assert reconstruction is not None

        # Store residual blocks separately so we can evaluate at the end and see how much each type of loss contributes.
        self.residuals_per_category = {}

        self.problem = pyceres.Problem()
        for image_id in self.config.image_ids:
            self.add_image_to_problem(image_id, arkit_precomputed, timestamps_per_image, reconstruction, loss)

        # Add loop closure to ensure multiple detections of same QR code are at the same position
        # TODO rotations should also be same
        # debug_first_qr = True
        # for qr_id, cam_space_detections in detections_per_qr.items():
        #     if len(cam_space_detections) >= 2:
        #         qr_detections_by_image_id = {}
        #         qr_world_points_per_image_id = {}
        #         for image_id, detection in zip(image_ids_per_qr[qr_id], cam_space_detections):
        #             if image_id not in qr_detections_by_image_id.keys():
        #                 qr_detections_by_image_id[image_id] = []

        #             qr_detections_by_image_id[image_id].append(detection)

        #             if image_id not in qr_world_points_per_image_id.keys():
        #                 qr_world_points_per_image_id[image_id] = []

        #             image = reconstruction.images[image_id]
        #             camera = reconstruction.cameras[image.camera_id]
        #             qr_center_camspace = detection.translation
        #             qr_center_imgspace = camera.img_from_cam(qr_center_camspace)

        #             pixel_radius = 100 # TODO calculate this better, or maybe check in world-space instead

        #             # Find list of 3D feature points on the floor around the QR code
        #             for point2D in image.points2D:
        #                 if not point2D.has_point3D():
        #                     continue

        #                 pixel_offset = norm(point2D.xy - qr_center_imgspace)
        #                 if pixel_offset > pixel_radius:
        #                     continue

        #                 point3D = reconstruction.points3D[point2D.point3D_id]
        #                 qr_world_points_per_image_id[image_id].append(point3D)

        #             if debug_first_qr:
        #                 print(f"QR {qr_id} in",
        #                       f"image {image_id} ({reconstruction.images[image_id].name}): center pixel:",
        #                       qr_center_imgspace, ", Cam-space center: ", qr_center_camspace,
        #                       f", {len(qr_world_points_per_image_id[image_id])} 3D features with height deviation",
        #                       f"{np.std([point.xyz[0] for point in qr_world_points_per_image_id[image_id]]):.6f}",
        #                       ", pose: ", detection)


        #         self.add_qr_detections_to_problem(reconstruction,
        #                                           qr_detections_by_image_id,
        #                                           qr_world_points_per_image_id,
        #                                           debug_first_qr)
        #         debug_first_qr = False

        for point3D_id in self.config.variable_point3D_ids:
            self.add_point_to_problem(point3D_id, reconstruction, loss)
        for point3D_id in self.config.constant_point3D_ids:
            self.add_point_to_problem(point3D_id, reconstruction, loss)
        self.parameterize_cameras(reconstruction)
        self.parameterize_points(reconstruction)
        return self.problem

    def set_up_solver_options(
        self, problem: pyceres.Problem, solver_options: pyceres.SolverOptions
    ):
        bundle_adjuster = pycolmap.BundleAdjuster(self.options, self.config)
        return bundle_adjuster.set_up_solver_options(problem, solver_options)

    # category and image id is just for plotting loss distribution per frame, to understand and improve the refinement algorithm
    def add_residual_block(self, category, cost, loss, parameter_blocks, image_id=None):
        if category not in self.residuals_per_category:
            self.residuals_per_category[category] = []

        self.residuals_per_category[category].append((cost, loss, parameter_blocks, image_id))
        self.problem.add_residual_block(cost, loss, parameter_blocks)

    def evaluate_loss_breakdown(self):

        loss_breakdown = {
            category: {
                "sum": 0.0,
                "count": 0
            } for category in self.residuals_per_category.keys()
        }

        loss_breakdown_per_image_id = {
            image_id: {
                category: {
                    "sum": 0.0,
                    "count": 0
                } for category in self.residuals_per_category.keys()
            } for image_id in self.config.image_ids
        }

        for category, datas in self.residuals_per_category.items():
            for data in datas:
                cost, loss, parameter_blocks, image_id = data

                residuals, jacobians = cost.evaluate(*parameter_blocks)
                if residuals is not None:
                    cost = norm(residuals) ** 2
                    if loss is not None:
                        cost = loss.evaluate(cost)[0] # First index is the modified cost ('loss' function downsizes outliers)
                    loss_breakdown[category]["sum"] += cost
                    loss_breakdown[category]["count"] += 1

                    if image_id is not None:
                        loss_breakdown_per_image_id[image_id][category]["sum"] += cost
                        loss_breakdown_per_image_id[image_id][category]["count"] += 1

        return loss_breakdown, loss_breakdown_per_image_id


    def is_constant_cam_pose(self, image_id):
        return (not self.options.refine_extrinsics) or self.config.has_constant_cam_pose(image_id)

    def is_constant_cam_position(self, image_id):
        return (not self.options.refine_extrinsics) or self.config.has_constant_cam_positions(image_id)


    def add_image_to_problem(
        self,
        image_id: int,
        arkit_precomputed,
        timestamps_per_image,
        reconstruction: pycolmap.Reconstruction,
        loss: pyceres.LossFunction,
    ):
        image = reconstruction.images[image_id]
        pose = image.cam_from_world

        #DEBUG
        if image_id == 50:
            print("ADD IMAGE", image_id, "with pose", pose)

        camera = reconstruction.cameras[image.image_id]
        constant_cam_pose = self.is_constant_cam_pose(image.image_id)

        # CUSTOM!
        if image_id > 1 and not constant_cam_pose:
            prev_image = reconstruction.images[image_id - 1]
            prev_pose = prev_image.cam_from_world

            arkit_offset_moved = arkit_precomputed[image_id]["offset_moved"]
            arkit_offset_rotated = arkit_precomputed[image_id]["offset_rotated"]
            arkit_gravity_direction = arkit_precomputed[image_id]["gravity_direction"]

            #print("GRAV:", arkit_gravity_direction)
            debugging = image_id == 50

            """
            if debugging:
                print(f"prev_arkit_cam_from_world: {prev_arkit_cam_from_world}")
                print(f"arkit_cam_from_world: {arkit_cam_from_world}")
                print(f"prev_arkit_world_position: {prev_arkit_world_position}")
                print(f"arkit_offset_moved: {arkit_offset_moved}")
                print(f"arkit_offset_rotated: {arkit_offset_rotated}")
            """

            # Pay less importance to frames where arkit moves too fast.
            # Fast movement is either prone to drift, or caused by a sudden spike where ARKit does its drift correction.
            # We want our optimization to ignore those bad ARKit spikes and rely more on visual features for those frames.
            # Gradually reduce the weight based on how fast ARKit camera moved this frame.

            delta_time = (timestamps_per_image[image.name] - timestamps_per_image[prev_image.name]) / 1.0e9
            arkit_speed = norm(arkit_offset_moved) / delta_time
            slow_speed = 1.3 # m / s

            reliability_factor = 1.0 if arkit_speed < slow_speed else 1.0 / (1 + 4 * (arkit_speed - slow_speed))

            #weight = 0.05 # First good result (saved here for reference)

            #weight = 0.05 + 0.2 * reliability_factor
            #weight = 0.05 * reliability_factor
            offset_weight = reliability_factor * self.refinement_config.get("distance_moved_loss_weight", 0.05)
            gravity_weight = self.refinement_config.get("gravity_loss_weight", 200.0)


            if(reliability_factor < 1 or image_id <= 5):
                print(f"img {image_id} :: delta_time: {delta_time:.5f},"
                      f"arkit_speed: {arkit_speed:.5f}, reliability: {reliability_factor:.5f},",
                      f"offset_weight: {offset_weight}, gravity_weight: {gravity_weight}")

            const_prev_pos = None
            const_prev_quat = None
            if self.is_constant_cam_pose(image.image_id - 1):
                const_prev_pos = prev_pose.translation
                const_prev_quat = prev_pose.rotation.quat
            elif self.is_constant_cam_position(image.image_id - 1):
                const_prev_pos = prev_pose.translation

            cost = DistanceMovedCostFunction(arkit_offset_moved, arkit_offset_rotated, arkit_gravity_direction,
                                             const_prev_pos=const_prev_pos,
                                             const_prev_quat=const_prev_quat,
                                             offset_weight=offset_weight, #0.05
                                             gravity_weight=gravity_weight,
                                             image_id_debug=image_id,
                                             debugging=debugging)

            params = [
                pose.translation,
                pose.rotation.quat
            ]
            if const_prev_pos is None:
                params.append(prev_pose.translation)
            if const_prev_quat is None:
                params.append(prev_pose.rotation.quat)

            self.add_residual_block("OffsetFromUnrefined", cost, None, params, image_id)

        num_observations = 0
        constant_cam_features = 0 # DEBUGGING
        for point2D in image.points2D:
            if not point2D.has_point3D():
                continue
            num_observations += 1
            if point2D.point3D_id not in self.point3D_num_observations:
                self.point3D_num_observations[point2D.point3D_id] = 0
            self.point3D_num_observations[point2D.point3D_id] += 1
            point3D = reconstruction.points3D[point2D.point3D_id]
            assert point3D.track.length() > 1
            if constant_cam_pose:
                cost = pycolmap.cost_functions.ReprojErrorCost(
                    camera.model, pose, point2D.xy
                )
                constant_cam_features += 1
                self.add_residual_block(
                    "FeaturePointReproj_ConstantCam",
                    cost, loss, [point3D.xyz, camera.params], image_id
                )
            else:
                cost = pycolmap.cost_functions.ReprojErrorCost(
                    camera.model, point2D.xy
                )
                self.add_residual_block(
                    "FeaturePointReproj",
                    cost,
                    loss,
                    [
                        pose.rotation.quat,
                        pose.translation,
                        point3D.xyz,
                        camera.params
                    ],
                    image_id
                )

        if constant_cam_pose:
            print(f"CONSTANT CAM image {image.image_id} has {constant_cam_features} feature residuals")
        if num_observations > 0:
            self.camera_ids.add(image.camera_id)
            # Set pose parameterization
            if not constant_cam_pose:
                self.problem.set_manifold(
                    pose.rotation.quat, pyceres.QuaternionManifold()
                )
                if self.config.has_constant_cam_positions(image_id):
                    constant_position_idxs = self.config.constant_cam_positions(
                        image_id
                    )
                    self.problem.set_manifold(
                        pose.translation,
                        pyceres.SubsetManifold(3, constant_position_idxs),
                    )

    def add_qr_detections_to_problem(self, reconstruction, detections_per_image_id, qr_world_points_per_image_id=None, debugging=False):

        # NOTE multiple detections can sometimes get rounded to the same image ID pose.
        # However, we must never add the same cam pose multiple times to the parameter blocks, or ceres throws an error
        # So, we store detections as an array with image_id as key.
        if len(detections_per_image_id) <= 1:
            print(f"Cannot add QR loop closure with less than two images. Skipping! (got {len(detections_per_image_id)} detections)")
            return

        const_positions = {}
        const_quaternions = {}
        parameter_blocks = []

        # Add cameras as either parameters or constants (depending on config. First 2 cams are constrained by default)
        for image_id in detections_per_image_id.keys():

            image = reconstruction.images[image_id]


            const_pos = False
            const_quat = False
            if self.is_constant_cam_pose(image_id):
                const_pos = True
                const_quat = True
            elif self.is_constant_cam_position(image_id):
                const_pos = True

            if const_pos:
                const_positions[image_id] = image.cam_from_world.translation
            else:
                parameter_blocks.append(image.cam_from_world.translation)

            if const_quat:
                const_quaternions[image_id] = image.cam_from_world.rotation.quat
            else:
                parameter_blocks.append(image.cam_from_world.rotation.quat)

        cost = CustomLoopClosureCostFunction(detections_per_image_id,
                                             qr_world_points_per_image_id=qr_world_points_per_image_id,
                                             weight=self.refinement_config.get("loop_closure_loss_weight", 0.1),
                                             const_positions=const_positions,
                                             const_quaternions=const_quaternions,
                                             debugging=debugging)


        self.add_residual_block("QrLoopClosure", cost, None, parameter_blocks)

        print(f"Added loop closure cost function for images: {detections_per_image_id.keys()}. Cam world poses: {[reconstruction.images[id].cam_from_world.inverse() for id in detections_per_image_id.keys()]}")


    def add_point_to_problem(
        self,
        point3D_id: int,
        reconstruction: pycolmap.Reconstruction,
        loss: pyceres.LossFunction,
    ):
        point3D = reconstruction.points3D[point3D_id]
        if point3D_id in self.point3D_num_observations:
            if (
                self.point3D_num_observations[point3D_id]
                == point3D.track.length()
            ):
                return
        else:
            self.point3D_num_observations[point3D_id] = 0
        for track_el in point3D.track.elements:
            if self.config.has_image(track_el.image_id):
                continue
            self.point3D_num_observations[point3D_id] += 1
            image = reconstruction.images[track_el.image_id]
            camera = reconstruction.cameras[image.camera_id]
            point2D = image.point2D(track_el.point2D_idx)
            if image.camera_id not in self.camera_ids:
                self.camera_ids.add(image.camera_id)
                self.config.set_constant_cam_intrinsics(image.camera_id)
            cost = pycolmap.cost_functions.ReprojErrorCost(
                camera.model, image.cam_from_world, point2D.xy
            )
            self.add_residual_block(
                "3DPointReproj", cost, loss, [point3D.xyz, camera.params]
            )

    def parameterize_cameras(self, reconstruction: pycolmap.Reconstruction):
        constant_camera = (
            (not self.options.refine_focal_length)
            and (not self.options.refine_principal_point)
            and (not self.options.refine_extra_params)
        )
        for camera_id in self.camera_ids:
            camera = reconstruction.cameras[camera_id]
            if constant_camera or self.config.has_constant_cam_intrinsics(
                camera_id
            ):
                self.problem.set_parameter_block_constant(camera.params)
                continue
            const_camera_params = []
            if not self.options.refine_focal_length:
                const_camera_params.extend(camera.focal_length_idxs())
            if not self.options.refine_principal_point:
                const_camera_params.extend(camera.principal_point_idxs())
            if not self.options.refine_extra_params:
                const_camera_params.extend(camera.extra_params_idxs())
            if len(const_camera_params) > 0:
                self.problem.set_manifold(
                    camera.params,
                    pyceres.SubsetManifold(
                        len(camera.params), const_camera_params
                    ),
                )

    def parameterize_points(self, reconstruction: pycolmap.Reconstruction):
        for (
            point3D_id,
            num_observations,
        ) in self.point3D_num_observations.items():
            point3D = reconstruction.points3D[point3D_id]
            if point3D.track.length() > num_observations:
                self.problem.set_parameter_block_constant(point3D.xyz)
        for point3D_id in self.config.constant_point3D_ids:
            point3D = reconstruction.points3D[point3D_id]
            self.problem.set_parameter_block_constant(point3D.xyz)


def dmt_ba_solve_bundle_adjustment(detections_per_qr,
                                   image_ids_per_qr,
                                   timestamps_per_image,
                                   arkit_precomputed,
                                   reconstruction,
                                   ba_options,
                                   ba_config,
                                   refinement_config={}):
    if refinement_config == None:
        refinement_config = {}

    bundle_adjuster = PyBundleAdjuster(ba_options, ba_config, refinement_config)
    bundle_adjuster.set_up_problem(
        detections_per_qr,
        image_ids_per_qr,
        timestamps_per_image,
        arkit_precomputed,
        reconstruction,
        ba_options.create_loss_function()
    )

    solver_options = bundle_adjuster.set_up_solver_options(
        bundle_adjuster.problem, ba_options.solver_options
    )

    initial_loss_breakdown, initial_loss_breakdown_per_image_id = bundle_adjuster.evaluate_loss_breakdown()
    print("------------")
    print("INITIAL LOSS BREAKDOWN:")
    print("\n".join([f"{category}: {loss}" for category, loss in initial_loss_breakdown.items()]))
    print("------------")

    summary = pyceres.SolverSummary()
    pyceres.solve(solver_options, bundle_adjuster.problem, summary)
    final_loss_breakdown, final_loss_breakdown_per_image_id = bundle_adjuster.evaluate_loss_breakdown()

    print("------------")
    print("INITIAL LOSS BREAKDOWN:")
    print("\n".join([f"{category}: {loss}" for category, loss in initial_loss_breakdown.items()]))
    print("------------")
    print("FINAL LOSS BREAKDOWN:")
    print("\n".join([f"{category}: {loss}" for category, loss in final_loss_breakdown.items()]))
    print("------------")

    print("Solved")
    loss_details = (
        initial_loss_breakdown,
        initial_loss_breakdown_per_image_id,
        final_loss_breakdown,
        final_loss_breakdown_per_image_id
    )

    return (summary, loss_details)


def prepare_ba_options():
    ba_options_tmp = pycolmap.BundleAdjustmentOptions()
    ba_options_tmp.solver_options.function_tolerance *= 10
    ba_options_tmp.solver_options.gradient_tolerance *= 10
    ba_options_tmp.solver_options.parameter_tolerance *= 10

    ba_options_tmp.solver_options.max_num_iterations = 50
    ba_options_tmp.solver_options.max_linear_solver_iterations = 200
    ba_options_tmp.print_summary = False
    return ba_options_tmp