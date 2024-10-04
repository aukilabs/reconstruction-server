import os
from kubernetes import client, config
from kubernetes.client import V1Volume, V1VolumeMount, V1ResourceRequirements, V1PodSpec, V1Pod, V1Container, V1PodTemplateSpec
from kubernetes.client.rest import ApiException
import time 
from tqdm import tqdm

# Load Kubernetes configuration (assuming Minikube is running)
config.load_kube_config()

# Initialize the Kubernetes API client
v1 = client.CoreV1Api()

# Base Docker image to use
docker_image = "docker.io/library/auki-archive:latest"  # Replace this with your image

# Set the base directory
base_dir = "/path/to/datasets"  # Replace with your directory path
output_path = "/path/to/refined/local"
# base_dir = "./test/datasets"  # Replace with your directory path
# output_path = "./test/refined/local"

def get_cpu_resource_required(folder_path):
    frame_path = os.path.join(folder_path, "Frames")
    img_num = len([entry for entry in os.listdir(frame_path) if os.path.isfile(os.path.join(frame_path, entry))])
    num_thread = 4 + img_num / 200
    return f"{int(num_thread)}"

# Create a pod for each folder in the base directory
def create_pod_for_folder(folder_name, folder_path):
    pod_name = f"{folder_name.replace('_', '-')}-pod"

    # Define volume and volume mount (mount the folder into the container)
    input_volume = V1Volume(
        name="input-folder",
        host_path=client.V1HostPathVolumeSource(path=folder_path)
        # {"path": folder_path}  # HostPath mounts the folder from the node's filesystem
    )
    input_volume_mount = V1VolumeMount(
        name="input-folder",
        mount_path=f"/{folder_name}"  # Mount it to `/input` inside the container
    )

    print(folder_path)

    # Define volume and volume mount (mount the folder into the container)
    output_volume = V1Volume(
        name="output-folder",
        host_path=client.V1HostPathVolumeSource(path=output_path)
        # {"path": output_path}  # HostPath mounts the folder from the node's filesystem
    )
    output_volume_mount = V1VolumeMount(
        name="output-folder",
        mount_path="/output"  # Mount it to `/input` inside the container
    )

    shm_volume = V1Volume(
        name="dshm",
        empty_dir=client.V1EmptyDirVolumeSource(
            medium="Memory",  # Use memory (RAM) instead of disk
            size_limit="2Gi"  # Set the shared memory size limit (adjust as needed)
        )
    )

    shm_volume_mount = V1VolumeMount(
        name="dshm",
        mount_path="/dev/shm"  # Mounting the volume to /dev/shm
    )

    cpu_resource = get_cpu_resource_required(folder_path)

    print(f"folder path: {folder_name}\tcpu: {cpu_resource}")

    # Define container with resource limits
    container = V1Container(
        name="container",
        image=docker_image,
        image_pull_policy="IfNotPresent",
        env = [
                {"name": "DP_DISABLE_HEALTHCHECKS",
                "value": "xids"},
                # {"name": "CUDA_MPS_ACTIVE_THREAD_PERCENTAGE",
                #  "value": "20"}
        ],
        resources=V1ResourceRequirements(
            limits={
                "cpu": cpu_resource,
                "memory": "6Gi",
                "nvidia.com/gpu": "1",  # This will allocate 1 GPU with 3GB of GPU RAM
            }
        ),
        volume_mounts=[
            input_volume_mount, 
            output_volume_mount, 
            # shm_volume_mount
        ],
        working_dir = "/app",
        command=["python3", "-m", "local_main"],
        args=[
            "--dataset_path", f"/{folder_name}",
            "--output_path", "/output",
            "--every_nth_image", "3" 
        ]
        # command=["ls", "-la"],
    )

    # Define the Pod spec
    pod_spec = V1PodSpec(
        containers=[container],
        volumes=[input_volume, output_volume], #,shm_volume],
        restart_policy="OnFailure"
    )

    # Create Pod object
    pod = V1Pod(
        api_version="v1",
        kind="Pod",
        metadata={"name": pod_name},
        spec=pod_spec
    )

    return pod, pod_name

def monitor_and_delete_pod(name):
    while True:
        pod = v1.read_namespaced_pod(name=name, namespace="default")
        pod_status = pod.status.phase
        if pod_status == "Succeeded" or pod_status == "Failed":
            print(f"Pod {name} completed with status: {pod_status}. Deleting...")
            v1.delete_namespaced_pod(name=name, namespace="default")
            print(f"Pod {name} deleted.")
            break
        else:
            # print(f"Pod {name} status: {pod_status}. Waiting...")
            time.sleep(5)

def get_pod_status(namespace=None):
    """
    Get the status of pods in the specified namespace (or all namespaces if none is specified).
    Returns a dictionary with pod names as keys and their statuses as values.
    """
    if namespace:
        pods = v1.list_namespaced_pod(namespace=namespace).items
    else:
        pods = v1.list_pod_for_all_namespaces().items
    
    pod_status = {}
    for pod in pods:
        pod_status[pod.metadata.name] = pod.status.phase
    return pod_status

def delete_completed_pods(namespace=None):
    """
    Delete pods that have reached the 'Succeeded' status in the specified namespace.
    """
    pod_status = get_pod_status(namespace)
    for pod_name, status in pod_status.items():
        if status == "Succeeded":
            try:
                if namespace:
                    v1.delete_namespaced_pod(name=pod_name, namespace=namespace)
                else:
                    pod_ns = pod_name.split('/')[0]  # Extract namespace from pod name
                    v1.delete_namespaced_pod(name=pod_name, namespace=pod_ns)
                print(f"Pod {pod_name} deleted.")
            except client.exceptions.ApiException as e:
                print(f"Failed to delete pod {pod_name}: {e}")

def monitor_pods(namespace=None):
    """
    Monitor the progress of all pods reaching the 'Completed' status.
    """
    print("Fetching pod details...")

    # Get initial pod statuses
    pod_status = get_pod_status(namespace)
    total_pods = len(pod_status)
    completed_pods = 0
    
    # Create a progress bar
    with tqdm(total=total_pods, desc="Monitoring Pods", unit="pod") as pbar:
        while completed_pods < total_pods:
            pod_status = get_pod_status(namespace)
            completed_pods = sum(1 for status in pod_status.values() if status == "Succeeded")
            pbar.n = completed_pods
            pbar.refresh()

            # Wait before the next check
            time.sleep(5)

    print("All pods have completed.")

def create_pods_for_folders(base_dir):
    pod_names = []
    # List all folders in the base directory
    for folder_name in os.listdir(base_dir):
        folder_path = os.path.join(base_dir, folder_name)

        if os.path.isdir(folder_path):
            pod, podname = create_pod_for_folder(folder_name, folder_path)
            try:
                # Create the pod in the default namespace
                v1.create_namespaced_pod(namespace="default", body=pod)
                print(f"Pod '{folder_name}-pod' created successfully.")
                pod_names.append(podname)
            except ApiException as e:
                print(f"Exception when creating pod for folder {folder_name}: {e}")


if __name__ == "__main__":
    start_time = time.time()
    create_pods_for_folders(base_dir)
    monitor_pods(namespace="default")
    
    # Delete pods that have completed
    delete_completed_pods(namespace = "default")

    print(f"Total time spent: {time.time() - start_time} seconds")