from pathlib import Path
from typing import NamedTuple

from hloc.triangulation import create_db_from_model, import_features, import_matches
from hloc import extract_features, match_features, pairs_from_retrieval, reconstruction
from utils.bundle_adjuster import PyBundleAdjuster


class RefinementPaths(NamedTuple):
    """Container for all paths used in refinement."""
    scan_folder: Path
    output: Path
    images: Path
    sfm_dir: Path
    colmap_rec: Path
    features: Path
    global_features: Path
    matches: Path
    sfm_pairs: Path
    log_path: Path


scan_folder_path = Path("/home/auki/Workspaces/sam_ws/data/data-drive/360-Experiments/test-vid-2")
output_path = Path("/home/auki/Workspaces/sam_ws/data/data-drive/360-Experiments/test-vid-2/output")

# global_feature_conf = extract_features.confs["netvlad"]

experiment_name = scan_folder_path.name

paths = RefinementPaths(
    scan_folder=scan_folder_path,
    output=output_path / experiment_name,
    images=scan_folder_path / 'Frames/',
    sfm_dir=output_path / experiment_name / 'sfm',
    colmap_rec=output_path / experiment_name / 'colmap_rec',
    features=output_path / experiment_name / 'sfm/features.h5',
    global_features=output_path / experiment_name / 'sfm/global_features.h5',
    matches=output_path / experiment_name / 'sfm/matches.h5',
    sfm_pairs=output_path / experiment_name / 'sfm/pairs-sfm.txt',
    log_path=output_path / experiment_name
)

references = [str(p.relative_to(paths.images)) for p in paths.images.iterdir()]


# Specify git ref explicitly since otherwise the automatic lookup fails occasionally
print("extracting global features")
global_feature_conf = extract_features.confs["eigenplaces"]
global_feature_conf["model"]["variant"] = "EigenPlaces:main"
global_feature_conf["output"] = paths.global_features
extract_features.main(
    global_feature_conf,
    paths.images,
    paths.sfm_dir,
    feature_path=paths.global_features,
    as_half=True,
    image_list=references,
)
print("matching global features")
pairs_from_retrieval.main(paths.global_features, paths.sfm_pairs, num_matched=5)


print("extracting local features")
feature_conf = extract_features.confs["aliked-n16"]
feature_conf["model"]["max_num_keypoints"] = 1024
feature_conf["model"]["detection_threshold"] = 0.3
feature_conf["model"]["nms_radius"] = 4
feature_conf["preprocessing"]["resize_max"] = 1024
feature_conf["output"] = paths.features
extract_features.main(
    feature_conf,
    paths.images,
    paths.sfm_dir,
    feature_path=paths.features,
    as_half=True,
    image_list=references,
    #overwrite=True
)

print("matching local features")
matcher_conf = match_features.confs["aliked+lightglue"]
matcher_conf["model"]["compile_network"] = True
match_features.main(
    matcher_conf, 
    paths.sfm_pairs, 
    features=paths.features, 
    export_dir=paths.sfm_dir,
    matches=paths.matches,
    #overwrite=True
)
# match_path = match_features.main(
#     matcher_conf, sfm_pairs, feature_conf["output"], outputs
# )

print("running reconstruction")
# reconstruction.main(paths.sfm_dir, paths.images, paths.sfm_pairs, paths.features, paths.matches)