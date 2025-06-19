import pycolmap
import pyceres
import numpy as np
from numpy.linalg import norm

from utils.data_utils import vec3_angle, is_floor_portal
from utils.cost_utils import DistanceMovedCostFunction, CustomLoopClosureCostFunction
from src.cost_functions import RelativeTransformationSE3CostFunction, RelativeTransformationSE3ViaObservationsCostFunction, PoseCenterConstraintCostFunction, FloorAlignmentCostFunction


class PyBundleAdjuster(object):
    # Python implementation of COLMAP bundle adjuster with pyceres
    def __init__(
        self,
        options: pycolmap.BundleAdjustmentOptions,
        config: pycolmap.BundleAdjustmentConfig,
        refinement_config: 'dict | None' = None
    ):
        self.options = options
        self.config = config
        self.refinement_config = refinement_config if refinement_config is not None else {}
        self.problem = pyceres.Problem()
        self.summary = pyceres.SolverSummary()
        self.camera_ids = set()
        self.point3D_num_observations = dict()
        self.featureless_camera_ids = set()

    def set_up_problem(
        self,
        reconstruction: pycolmap.Reconstruction,
        loss: pyceres.LossFunction,
        timestamp_per_image,
        detections_per_qr=None,
        image_ids_per_qr=None,
        arkit_precomputed=None,
        verbose=False,
    ):
        assert reconstruction is not None

        # Store residual blocks separately so we can evaluate at the end and see how much each type of loss contributes.
        self.residuals_per_category = {}

        self.problem = pyceres.Problem()
        for image_id in self.config.image_ids:
            self.add_image_to_problem_upd(image_id, reconstruction, loss, timestamp_per_image, arkit_precomputed)

        # Add loop closure to ensure multiple detections of same QR code are at the same position
        # TODO rotations should also be same
        debug_first_qr = verbose
        if detections_per_qr is not None:
            for qr_id, cam_space_detections in detections_per_qr.items():
                #debug_this_qr = debug_first_qr
                debug_this_qr = (debug_first_qr or qr_id in [
                    "TA7DPLXURM8" # Paste more IDs here if extra debugging is needed
                ])
                if len(cam_space_detections) >= 2:
                    qr_detections_by_image_id = {}
                    qr_world_points_per_image_id = {}
                    for image_id, detection in zip(image_ids_per_qr[qr_id], cam_space_detections):

                        image = reconstruction.images[image_id]
                        camera = reconstruction.cameras[image.camera_id]
                        qr_center_camspace = detection.translation

                        angle_from_cam_forward = vec3_angle(qr_center_camspace, np.array([0,0,1]))
                        """
                        if angle_from_cam_forward > 10:
                            if verbose:
                                print(f"QR {qr_id} in image {image_id} is more than 10 deg away from cam center, SKIP loop closure.",
                                    f"(angle_from_cam_forward={angle_from_cam_forward}, qr_center_camspace={qr_center_camspace})")
                        else:
                        """
                        if True: # TODO not good practise to do this

                            if image_id not in qr_detections_by_image_id.keys():
                                qr_detections_by_image_id[image_id] = []

                            qr_detections_by_image_id[image_id].append(detection)

                            if image_id not in qr_world_points_per_image_id.keys():
                               qr_world_points_per_image_id[image_id] = []

                            qr_center_imgspace = camera.img_from_cam(qr_center_camspace)

                            # Don't do world point distance scaling for visually unreliable images
                            if image.camera_id not in self.featureless_camera_ids:
                               pixel_radius = 70 # TODO calculate this better, or maybe check in world-space instead
                            
                               # Find list of 3D feature points on the floor around the QR code
                               for point2D in image.points2D:
                                   if not point2D.has_point3D():
                                       continue
                            
                                   pixel_offset = norm(point2D.xy - qr_center_imgspace)
                                   if pixel_offset > pixel_radius:
                                       continue
                            
                                   point3D = reconstruction.points3D[point2D.point3D_id]
                                   qr_world_points_per_image_id[image_id].append(point3D)
                            
                            if debug_this_qr:
                               print(f"QR {qr_id} in",
                                   f"image {image_id} ({reconstruction.images[image_id].name}): center pixel:",
                                   qr_center_imgspace, ", Cam-space center: ", qr_center_camspace,
                                   f", {len(qr_world_points_per_image_id[image_id])} 3D features with height deviation",
                                   f"{np.std([point.xyz[0] for point in qr_world_points_per_image_id[image_id]]):.6f}",
                                   ", pose: ", detection)


                    added = self.add_qr_detections_to_problem_upd(
                        reconstruction,
                        qr_detections_by_image_id,
                        qr_world_points_per_image_id,
                        debug_this_qr
                    )
                    if verbose and not added:
                        print("Skipped adding loop closure for QR", qr_id)

                    debug_first_qr = False

        def needs_manifold(param):
            return self.problem.has_parameter_block(param) and not self.problem.is_parameter_block_constant(param)

        for image_id in self.config.image_ids:
            image = reconstruction.images[image_id]
            pose = image.cam_from_world

            # Depending on some different conditions above,
            # some images will be skipped in either of the loss conditions.
            # If an image is not part of any losses, it is not a parameter and we cannot add manifolds.
            # If it is part of any (or more), we must add it. (makes sure quaterions stay valid etc)

            if needs_manifold(pose.rotation.quat):
                self.problem.set_manifold(
                    pose.rotation.quat, pyceres.QuaternionManifold()
                )

            if needs_manifold(pose.translation) and self.config.has_constant_cam_positions(image_id):
                constant_position_idxs = self.config.constant_cam_positions(
                    image_id
                )
                self.problem.set_manifold(
                    pose.translation,
                    pyceres.SubsetManifold(3, constant_position_idxs),
                )

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

        raw_costs_per_category = {} # Print raw costs before applying loss function, to help balancing huber, cauchy loss etc.
        for category, datas in self.residuals_per_category.items():
            for data in datas:
                cost, loss, parameter_blocks, image_id = data

                residuals, jacobians = cost.evaluate(*parameter_blocks)
                if residuals is not None:
                    raw_residuals = np.abs(residuals)

                    cost = norm(residuals) ** 2
                    
                    if category not in raw_costs_per_category:
                        raw_costs_per_category[category] = []
                    raw_costs_per_category[category].append(cost)

                    if loss is not None:
                        cost = loss.evaluate(cost)[0] # First index is the modified cost ('loss' function downsizes outliers)
                    cost /= 2
                    loss_breakdown[category]["sum"] += cost
                    loss_breakdown[category]["count"] += 1

                    if image_id is not None:
                        loss_breakdown_per_image_id[image_id][category]["sum"] += cost
                        loss_breakdown_per_image_id[image_id][category]["count"] += 1

        for category, raw_costs in raw_costs_per_category.items():
            if category not in loss_breakdown:
                loss_breakdown[category]["outlier_80perc"] = 0.0
                loss_breakdown[category]["outlier_90perc"] = 0.0
                loss_breakdown[category]["outlier_95perc"] = 0.0
                continue
            loss_breakdown[category]["outlier_90perc"] = np.percentile(raw_costs, 90)
            loss_breakdown[category]["outlier_80perc"] = np.percentile(raw_costs, 80)
            loss_breakdown[category]["outlier_95perc"] = np.percentile(raw_costs, 95)


        return loss_breakdown, loss_breakdown_per_image_id


    def is_constant_cam_pose(self, image_id):
        return (not self.options.refine_extrinsics) or self.config.has_constant_cam_pose(image_id)

    def is_constant_cam_position(self, image_id):
        return (not self.options.refine_extrinsics) or self.config.has_constant_cam_positions(image_id)


    def add_image_to_problem(
        self,
        image_id: int,
        arkit_precomputed,
        timestamp_per_image,
        reconstruction: pycolmap.Reconstruction,
        loss: pyceres.LossFunction,
    ):
        image = reconstruction.images[image_id]
        pose = image.cam_from_world

        #DEBUG
        #if image_id == 50:
        #if image_id >= 171 and image_id <= 173:
        #if image_id >= 115 and image_id <= 117:
        #    print("ADD IMAGE", image_id, "with pose", pose)

        camera = reconstruction.cameras[image.image_id]
        constant_cam_pose = self.is_constant_cam_pose(image.image_id)

        # CUSTOM!
        if image_id > 1 and not constant_cam_pose:
            prev_image = reconstruction.images[image_id - 1]
            prev_pose = prev_image.cam_from_world

            if image_id not in arkit_precomputed:
                return

            arkit_offset_moved = arkit_precomputed[image_id]["offset_moved"]
            arkit_offset_rotated = arkit_precomputed[image_id]["offset_rotated"]
            arkit_gravity_direction = arkit_precomputed[image_id]["gravity_direction"]

            #print("GRAV:", arkit_gravity_direction)
            debugging = image_id == 20
            #debugging = image_id >= 171 and image_id <= 173
            #debugging = False #image_id >= 115 and image_id <= 117

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

            delta_time = (timestamp_per_image[image.name] - timestamp_per_image[prev_image.name]) / 1.0e9
            arkit_speed = norm(arkit_offset_moved) / delta_time

            if abs(delta_time) > 1.0 and arkit_speed < 0.0001:
                print(image.name, ": FIRST IMAGE in this chunk probably, NOT applying OffsetFromUnrefined cost.")
            else:
                slow_speed = 1.0 #1.3 # m / s

                reliability_factor = 1.0 # if arkit_speed < slow_speed else 1.0 / (1 + 5 * (arkit_speed - slow_speed))

                offset_weight = reliability_factor * self.refinement_config.get("distance_moved_loss_weight", 0.1) #1000.0)
                gravity_weight = self.refinement_config.get("gravity_loss_weight", 0.1)


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

                cost = DistanceMovedCostFunction(
                    arkit_offset_moved, 
                    arkit_offset_rotated, 
                    arkit_gravity_direction,
                    const_prev_pos=const_prev_pos,
                    const_prev_quat=const_prev_quat,
                    offset_weight=offset_weight,
                    gravity_weight=gravity_weight,
                    image_id_debug=image_id,
                    debugging=debugging
                )

                params = [
                    pose.translation,
                    pose.rotation.quat
                ]
                if const_prev_pos is None:
                    params.append(prev_pose.translation)
                if const_prev_quat is None:
                    params.append(prev_pose.rotation.quat)

                self.add_residual_block("OffsetFromUnrefined", cost, None, params, image_id)

        self.camera_ids.add(image.camera_id)

    def add_image_to_problem_upd(
        self,
        image_id: int,
        reconstruction: pycolmap.Reconstruction,
        loss: pyceres.LossFunction,
        timestamp_per_image,
        arkit_precomputed=None,
    ):
        image = reconstruction.images[image_id]
        pose = image.cam_from_world
        camera = reconstruction.cameras[image.camera_id]

        constant_cam_pose = self.is_constant_cam_pose(image.image_id)

        num_observations = 0
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
                self.add_residual_block("3DPointReproj", cost, loss, [point3D.xyz, camera.params], image_id)
            else:
                cost = pycolmap.cost_functions.ReprojErrorCost(
                    camera.model, point2D.xy
                )
                self.add_residual_block("3DPointReproj", cost, loss, [
                        pose.rotation.quat,
                        pose.translation,
                        point3D.xyz,
                        camera.params], image_id)
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
        else:
            self.featureless_camera_ids.add(image.camera_id)

        use_arkit_centerdist = self.refinement_config.get('use_arkit_centerdist', False)
        if not constant_cam_pose and use_arkit_centerdist:
            arkit_cam_from_world = arkit_precomputed[image_id]['cam_from_world']
            arkit_cam_center = arkit_cam_from_world.rotation.inverse() * -arkit_cam_from_world.translation

            centerdist_weight = self.refinement_config.get('centerdist_weight', 1.0)
            cost = PoseCenterConstraintCostFunction(arkit_cam_center, (centerdist_weight, centerdist_weight, centerdist_weight))
            params = [
                pose.rotation.quat,
                pose.translation
            ]

            self.add_residual_block("OffsetFromUnrefinedCenter", cost, None, params, image_id)

            self.camera_ids.add(image.camera_id)


        # CUSTOM!
        use_relposes = self.refinement_config.get('add_rel_constraints', False)
        has_spike = image_id in arkit_precomputed and arkit_precomputed[image_id].get("arkit_spike", False)
        if use_relposes and image_id > 1 and not constant_cam_pose and not has_spike \
           and image_id in reconstruction.images and (image_id - 1) in reconstruction.images:
           
            prev_image = reconstruction.images[image_id - 1]
            prev_pose = prev_image.cam_from_world

            use_precomputed_relposes = self.refinement_config.get('use_arkit_relposes', False)
            if use_precomputed_relposes:
                if image_id in arkit_precomputed and prev_image.image_id in arkit_precomputed:
                    #arkit_offset_moved = arkit_precomputed[image_id]["offset_moved"]
                    #arkit_offset_rotated = arkit_precomputed[image_id]["offset_rotated"]
                    #relpose = pycolmap.Rigid3d(arkit_offset_rotated, arkit_offset_moved)
                    arkit_prev_pose = arkit_precomputed[prev_image.image_id]["cam_from_world"]
                    arkit_pose = arkit_precomputed[image_id]["cam_from_world"]
                    relpose = arkit_prev_pose.inverse() * arkit_pose
                else:
                    relpose = prev_pose.inverse() * pose
            else:
                relpose = prev_pose.inverse() * pose

            cov_scale = self.refinement_config.get('rel_se3_pose_cov_scale', 1.0)
            # optional, to trust rotation more (or less) than translation
            cov_scale_rot = self.refinement_config.get('rel_se3_pose_cov_scale_rot', cov_scale)
            cov = np.eye(6)
            cov[:3,:3] /= cov_scale_rot
            cov[3:,3:] /= cov_scale
            cost = RelativeTransformationSE3CostFunction(relpose.rotation.quat, relpose.translation, cov)
            params = [
                pose.rotation.quat,
                pose.translation,
                prev_pose.rotation.quat,
                prev_pose.translation
            ]

            self.add_residual_block("OffsetFromUnrefined", cost, None, params, image_id)

            if self.is_constant_cam_pose(image.image_id - 1):
                self.problem.set_parameter_block_constant(prev_pose.rotation.quat)
                self.problem.set_parameter_block_constant(prev_pose.translation)
            elif self.is_constant_cam_position(image.image_id - 1):
                self.problem.set_parameter_block_constant(prev_pose.translation)

            self.camera_ids.add(image.camera_id)

    def add_qr_detections_to_problem(self, reconstruction, detections_per_image_id, qr_world_points_per_image_id=None, debugging=False):

        # NOTE multiple detections can sometimes get rounded to the same image ID pose.
        # However, we must never add the same cam pose multiple times to the parameter blocks, or ceres throws an error
        # So, we store detections as an array with image_id as key.
        if len(detections_per_image_id) <= 1:
            print(f"Cannot add QR loop closure with less than two images. Skipping! (got {len(detections_per_image_id)} detections)")
            return False

        for i, image_id_i in enumerate(list(detections_per_image_id.keys())[:-1]):
            for j, image_id_j in enumerate(list(detections_per_image_id.keys())[i+1:]):

                const_positions = {}
                const_quaternions = {}
                parameter_blocks = []

                one_pair = {
                    image_id_i: detections_per_image_id[image_id_i],
                    image_id_j: detections_per_image_id[image_id_j]
                }

                # Add cameras as either parameters or constants (depending on config. First 2 cams are constrained by default)
                for image_id in one_pair.keys():
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


                cost = CustomLoopClosureCostFunction(
                    one_pair,
                    qr_world_points_per_image_id=None, #qr_world_points_per_image_id,
                    weight=self.refinement_config.get("loop_closure_loss_weight", 1.0),
                    const_positions=const_positions,
                    const_quaternions=const_quaternions,
                    debugging=debugging
                )


                self.add_residual_block("QrLoopClosure", cost, None, parameter_blocks)

                print(f"Added loop closure cost function for images: {one_pair.keys()}. Cam world poses: {[reconstruction.images[id].cam_from_world.inverse() for id in one_pair.keys()]}")

        return True

    def add_qr_detections_to_problem_upd(self, reconstruction, detections_per_image_id, qr_world_points_per_image_id=None, debugging=False):

        # NOTE multiple detections can sometimes get rounded to the same image ID pose.
        # However, we must never add the same cam pose multiple times to the parameter blocks, or ceres throws an error
        # So, we store detections as an array with image_id as key.
        if len(detections_per_image_id) <= 1:
            print(f"Cannot add QR loop closure with less than two images. Skipping! (got {len(detections_per_image_id)} detections)")
            return False
        
        # Bring floor portals to height 0 and flatten their rotation
        floor_height_weight = self.refinement_config.get('floor_height_weight', 1.0)
        floor_direction_weight = self.refinement_config.get('floor_direction_weight', 1.0)

        for i, image_id in enumerate(detections_per_image_id.keys()):
            assert len(detections_per_image_id[image_id]) == 1

            detection_pose = detections_per_image_id[image_id][0]
            detection_pose_world = reconstruction.images[image_id].cam_from_world.inverse() * detection_pose
            if not is_floor_portal(detection_pose_world):
                continue
            
            cost = FloorAlignmentCostFunction(
                detection_pose.rotation.quat,
                detection_pose.translation,
                floor_height_weight,
                floor_direction_weight
            )
            
            params = [
                reconstruction.images[image_id].cam_from_world.rotation.quat,
                reconstruction.images[image_id].cam_from_world.translation
            ]
            
            self.add_residual_block("QrFloorAlignment", cost, None, params, image_id)
            
            if debugging:
                print(f"Added floor alignment constraint for QR in image {image_id}")
        
        # Loop closure for multiple detections of same QR code
        for i, image_id_i in enumerate(list(detections_per_image_id.keys())[:-1]):
            assert len(detections_per_image_id[image_id_i]) == 1
            for j, image_id_j in enumerate(list(detections_per_image_id.keys())[i+1:]):
                assert len(detections_per_image_id[image_id_j]) == 1
                cov_scale = self.refinement_config.get('rel_qr_pose_cov_scale', 1.0)
                cost = RelativeTransformationSE3ViaObservationsCostFunction(
                    detections_per_image_id[image_id_i][0].rotation.quat,
                    detections_per_image_id[image_id_i][0].translation,
                    detections_per_image_id[image_id_j][0].rotation.quat,
                    detections_per_image_id[image_id_j][0].translation, 
                    np.eye(6) / cov_scale
                )
                params = [
                    reconstruction.images[image_id_j].cam_from_world.rotation.quat,
                    reconstruction.images[image_id_j].cam_from_world.translation,
                    reconstruction.images[image_id_i].cam_from_world.rotation.quat,
                    reconstruction.images[image_id_i].cam_from_world.translation
                ]

                self.add_residual_block("QrLoopClosure", cost, None, params, image_id_i)

                for image_id in (image_id_i, image_id_j):
                    if self.is_constant_cam_pose(image_id):
                        self.problem.set_parameter_block_constant(reconstruction.images[image_id].cam_from_world.rotation.quat)
                        self.problem.set_parameter_block_constant(reconstruction.images[image_id].cam_from_world.translation)
                    elif self.is_constant_cam_position(image_id):
                        self.problem.set_parameter_block_constant(reconstruction.images[image_id].cam_from_world.translation)

        return True


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
            image = reconstruction.images[track_el.image_id]
            if image.camera_id in self.featureless_camera_ids:
                continue

            self.point3D_num_observations[point3D_id] += 1

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
                # Cannot mark as constant for featureless cams since then there is no
                # cost function that even uses the parameters, and the line below would fail
                if camera_id not in self.featureless_camera_ids:
                    # CONSTANT INTRINSICS
                    self.problem.set_parameter_block_constant(camera.params)

                continue

            const_camera_params = []
            if not self.options.refine_focal_length:
                const_camera_params.extend(camera.focal_length_idxs())
            if not self.options.refine_principal_point:
                const_camera_params.extend(camera.principal_point_idxs())
            if not self.options.refine_extra_params:
                const_camera_params.extend(camera.extra_params_idxs())
            if len(const_camera_params) > 0 and camera_id not in self.featureless_camera_ids:
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
            if point3D.track.length() > num_observations and num_observations > 0:
                try:
                    self.problem.set_parameter_block_constant(point3D.xyz)
                except:
                    print("FAILURE, num_observations =", num_observations, ", track length =", point3D.track.length())
                    raise
        for point3D_id in self.config.constant_point3D_ids:
            point3D = reconstruction.points3D[point3D_id]
            self.problem.set_parameter_block_constant(point3D.xyz)
