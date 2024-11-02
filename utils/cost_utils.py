import pycolmap
import pyceres
from numpy.linalg import norm
import numpy as np

from utils.data_utils import vec3_angle


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

            i = 2
            if self.const_prev_pos is not None:
                prev_pos = self.const_prev_pos
            else:
                prev_pos = np.lib.stride_tricks.as_strided(params[i], strides=(8,))[0:3]
                i += 1

            if self.const_prev_quat is not None:
                prev_quat = self.const_prev_quat
            else:
                prev_quat = np.lib.stride_tricks.as_strided(params[i], strides=(8,))[0:4]
                i += 1

            cam_from_world = pycolmap.Rigid3d(pycolmap.Rotation3d(quat), pos)
            prev_cam_from_world = pycolmap.Rigid3d(pycolmap.Rotation3d(prev_quat), prev_pos)

            offset = cam_from_world * prev_cam_from_world.inverse()

            position_cost = self.offset_weight * norm(self.arkit_offset_moved - offset.translation)

            angle_offset = offset.rotation.angle_to(self.arkit_offset_rotated)
            angle_cost = self.offset_weight * angle_offset * 10

            # Trust the gravity more from ARKit. That is already pretty accurate.
            gravity_direction = np.matmul(cam_from_world.matrix(), np.array([-1.0, 0.0, 0.0, 0.0]).transpose())[:3]
            gravity_cost = self.gravity_weight * vec3_angle(self.arkit_gravity_direction, gravity_direction)

            if debug_print:
                print(f"Iteration {self.debug_print_iteration} gravity cost: {gravity_cost} (position: {position_cost} + angle: {angle_cost} + gravity: {gravity_cost})  :::  offset moved: {offset.translation}, arkit offset: {self.arkit_offset_moved}")

            return position_cost + angle_cost + gravity_cost

        debug_print = self.debugging and (self.debug_print_iteration % 25 == 0 or self.debug_print_iteration <= 10)
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
                    new_params = [np.lib.stride_tricks.as_strided(block, strides=(8,)).copy() for block in parameters]
                    dx = 0.0001
                    new_params[parameter_block_index][parameter_index] += dx
                    new_residual = calc_loss(new_params)
                    dy = new_residual - residuals[0]
                    np.lib.stride_tricks.as_strided(jacobians[parameter_block_index], strides=(8,))[parameter_index] = dy / dx
                    #if debug_print:
                    #    print(f"J[{parameter_block_index}][{parameter_index}]: dy={dy:.6f}, dx={dy:.6f}, dy/dx={dy/dx:.6f},",
                    #          f"new_residual={new_residual}, old_residual={residuals[0]}")

        if debug_print:
            print("OffsetFromUnrefined Jacobians:", jacobians)


        return True
    

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
        if (num_cameras <= 1 and len(self.const_positions) == 0 and len(self.const_quaternions) == 0):
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
                    translation = np.lib.stride_tricks.as_strided(params[i], strides=(8,))[0:3]
                    i += 1

                if image_id in self.const_quaternions.keys():
                    quaternion = self.const_quaternions[image_id]
                else:
                    quaternion = np.lib.stride_tricks.as_strided(params[i], strides=(8,))[0:4]
                    i += 1

                cam_from_world = pycolmap.Rigid3d(pycolmap.Rotation3d(quaternion), translation)

                for detection in self.detections_per_image_id[image_id]:
                    world_pose = cam_from_world.inverse() * detection
                    world_positions.append(world_pose.translation)
                    world_up = np.array(world_pose.rotation * ([1.0, 0.0, 0.0]))
                    world_up_vecs.append(world_up)
                    world_right_vecs.append(world_pose.rotation * ([0.0, 1.0, 0.0]))

                    # Loop through 3D points which are part of the QR code
                    # We want all of those to be "on" the QR code in world space. Not above or below.
                    # This ensures the refinement keeps the right scale.
                    # Only looking at visuals gives us a scale-invariant output but since we know
                    # the physical size of each QR code, we also know how far away from the camera it is.
                    # If the 3D reconstruction gets too small or big, the floor and all detected 3D points
                    # would be at the wrong depth.

                    """
                    if self.qr_world_points_per_image_id is None or len(self.qr_world_points_per_image_id[image_id]) < 10:
                        continue

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
                    """

            #pos_deviations = np.std(world_positions, axis=0)
            #pos_deviation = np.mean(pos_deviations)
            pos_deviation = norm(world_positions[0] - world_positions[1])
            height_loss = 100 * ((world_positions[0][0] * world_positions[0][0]) + (world_positions[1][0] * world_positions[1][0]))
            up_deviations = np.std(world_up_vecs, axis=0)
            up_deviation = np.mean(up_deviations)
            right_deviations = np.std(world_right_vecs, axis=0)
            right_deviation = np.mean(right_deviations)
            right_deviation_loss = right_deviation * 100


            # Make sure the 3D reconstructed floor stays at the same distance to the cam as when the app detected it.
            # Otherwise the reconstructed world can grow or shrink.
            # This works since we know the physical size of portals.
            qr_distance_loss = 0.0 #np.mean(mean_height_diffs) if len(mean_height_diffs) > 0 else 0.0
            #qr_distance_loss *= 10

            flatness_loss = 0.0
            for up in world_up_vecs:
                flatness_loss += vec3_angle(up, np.array([1.0, 0.0, 0.0]))
            if len(world_up_vecs) > 0:
                flatness_loss /= len(world_up_vecs)

            pos_deviation_loss = pos_deviation * 100

            if debug_print:
                print(f"Loop Closure deviation {pos_deviation:.6f} from {len(world_positions)} detections.",
                      f"Const pos [{','.join(str(id) for id in self.const_positions.keys())}].",
                      f"Const rot [{','.join(str(id) for id in self.const_quaternions.keys())}].",
                      f"pos_deviation_loss={pos_deviation_loss:.6f},",
                      #f"qr_distance_loss={qr_distance_loss:.6f},",
                      f"flatness_loss={flatness_loss:.6f},",
                      f"right_deviation_loss={right_deviation_loss:.6f},",
                      f"mean_height_diffs={mean_height_diffs},",
                      f"up_deviations={up_deviations}, right_deviations={right_deviations},",
                      #f"pos_deviations={pos_deviations},",
                      f"world_positions={world_positions}, world_ups={world_up_vecs}")


            # TODO move this to separate residual:
            #return (pos_deviation + up_deviation + right_deviation) * self.weight * 10000
            #return (pos_deviation_loss + flatness_loss + qr_distance_loss) * self.weight
            return (pos_deviation_loss + flatness_loss + right_deviation_loss) * self.weight

        debug_print = self.debugging and (self.debug_print_iteration % 25 == 0 or self.debug_print_iteration <= 10)

        residuals[0] = calc_loss(parameters, debug_print)

        self.debug_print_iteration += 1

        if jacobians is not None:  # check for Null
            for parameter_block_index in range(len(jacobians)):
                for parameter_index in range(len(jacobians[parameter_block_index])):
                    # Evaluates the cost function again but with a small increase on one parameter. Then dy/dx to approximate slope.
                    new_params = [np.lib.stride_tricks.as_strided(block, strides=(8,)).copy() for block in parameters]
                    dx = 0.00001
                    new_params[parameter_block_index][parameter_index] += dx
                    new_residual = calc_loss(new_params)
                    dy = new_residual - residuals[0]
                    np.lib.stride_tricks.as_strided(jacobians[parameter_block_index], strides=(8,))[parameter_index] = dy / dx

        if debug_print:
            print("LoopClosure Jacobians:", jacobians)

        return True