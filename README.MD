#### Build docker
```
docker buildx build -t docker.io/library/auki-archive:latest --load  .
```


#### Local refinement for single scan
```
docker run \
--gpus all \
--shm-size=512m \
-v /path/to/jobs/:/path/to/jobs/ \
-it auki-archive:latest \
main \
--dataset_path /path/to/jobs/my_domain_job/datasets/dmt_scan_2024-06-26_10-29-57 \
--output_path /path/to/jobs/my_domain_job/refined/local \
--every_nth_image 2 \
--remove_outputs
```

#### Local refinement for all scans within a folder
```
bash ./scripts/local_run_all.sh
```

#### Global refinement pipeline using refined outputs

```
docker run \
--gpus all \
-v /path/to/jobs/:/path/to/jobs/ \
-it auki-archive:latest \
global_stitching_main \
--data_dir ./datasets \
--all_poses \
--all_observations \
--use_refined_outputs
```

#### Global refinement pipeline, basic pointcloud stitch only

```
docker run \
--gpus all \
-v /path/to/jobs/:/path/to/jobs/ \
-it auki-archive:latest \
global_stitching_main \
--data_dir ./datasets \
--all_poses \
--all_observations \
--use_refined_outputs \
--add_3dpoints \
--basic_stitch_only
```