# Deployment

The Reconstruction server is available on [Docker Hub](https://hub.docker.com/r/aukilabs/reconstruction-node). Both deployment options are Docker-based.

## Initial Setup

1. First check that your NVIDIA driver and CUDA toolkit meet the requirements in the [Minimum Requirements](minimum-requirements.md) and update as needed:
   ```shell
   nvidia-smi
   ```
   This should output information about your GPU. If not, please double check that your installed CUDA and driver versions are correct, and restart your computer.

2. If you have a static IP, allow inbound TCP traffic to port 8080. Optionally also configure a domain name for it.

   Or, if your IP is not static, you need to set up Dynamic DNS pointing to your IP.

   One easy alternative if you don't have a static IP is to use ngrok:
   <details>
   <summary><strong>Ngrok Setup (optional)</strong></summary>

   1. **Install ngrok**  
      Follow [this beginner's tutorial 🔗](https://medium.com/@thexpertdev/ngrok-tutorial-for-beginners-how-to-expose-localhost-to-the-internet-and-test-webhooks-70845654fced) to install ngrok on your system.

   2. **Create a static domain**  
      On the ngrok website, set up a static domain as shown below:  
      <img src="images/ngrok-domains-page.png" alt="ngrok domains page" style="max-width:400px;">

   3. **Expose your server**  
      Run the following command (replace with your own ngrok domain):
      ```shell
      ngrok http --url my-cool-address.ngrok-free.app 8080
      ```

   <br/>

   💡 **NOTE:** If you restart your computer or server, you must ensure ngrok is running again, or your server will not be reachable.

   </details>

3. Disable power-saving settings like automatic sleep or standby mode, to keep your computer on and able to receive jobs.

## Option 1 — Use the prebuilt image (recommended)

Start Docker using the below command, ❗**including all flags**❗
```shell
docker run --gpus all --shm-size 512m -p 8080:8080 -d aukilabs/reconstruction-node:stable -cpu-workers 2 -port :8080 -api-key aukilabs123
```

💡 **NOTE 1:** Leave the -api-key as is, or use any non-sensitive phrase. During the community beta, you will need to provide this key to Auki Labs. This key is just an extra gate for incoming jobs, not used to access any user data.

💡 **NOTE 2:** if your system has an older CPU or less RAM and you notice any issues, you may try to reduce the `-cpu-workers` to 1, or even 0 (to run only on the main thread).

### Verification ✅

1. After deploying, please ensure the server started correctly by running
   ```shell
   docker ps
   ```
   This should show your newly started docker container, with the STATUS showing `Up 45 seconds` or similar.
   Copy the container ID of your server, then run:
   ```shell
   docker logs <container_id>
   ```
   You should see something like `{"time": ..... , "[Server running on  :8080]"}`.

2. Ensure your GPU and CUDA works correctly (using the container ID from above):
   ```shell
   docker exec <container_id> python3 -c "import torch; print(torch.cuda.get_device_name(0) if torch.cuda.is_available() else 'CUDA not found')"
   ```
   This should output the name of your main CUDA-supported GPU. If not, please double-check your setup, or see **Troubleshooting**.

3. Make sure the server is reachable on a public IP or URL.
   Open your browser and navigate to your URL + /jobs,
   e.g. `https://my_amazing_node.ngrok.com/jobs` or `http://162.88.88.88:8080/jobs` \
   This should show a list of reconstruction jobs, initially just `[]` (an empty list).


## Option 2 — Build Docker image from source

### Building Docker

> **NOTE:** On Mac with Apple Silicon, the --platform flag is needed. Although running the image with CUDA won't work on Mac, the image can still run on a cloud server for example, pulling from the docker hub.

```bash
# Linux computer or deploy to Linux server
docker buildx build --platform linux/amd64 -t {/your/docker/repo}:latest --load -f docker/Dockerfile .

# Jetson Device
DOCKER_BUILDKIT=1 docker buildx build --push --platform linux/arm64 -t {/your/docker/repo}:latest -f Dockerfile.jetson .
```

Run the image as described in Option 1, and follow the same verification steps.

## Troubleshooting ⚠️

Here are some common issues you may encounter, with suggested fixes:

### GPU not detected
- **Symptom:** Server starts but jobs fail, GPU not utilised, or `CUDA not found`.
- **Fixes:**  
  - Ensure you started Docker with `--gpus all`.
  - Check your driver and CUDA toolkit versions against [Minimum Requirements](minimum-requirements.md).
  - Run `nvidia-smi` on host to confirm your GPU is visible.
  - Run `nvcc --version` to confirm your CUDA toolkit is installed, and with the correct version.
  - Restart your computer and try again.

### Corrupted model cache
- **Symptom:** Large model file (e.g. “Eigenplaces”) download fails or jobs fail during image matching.
- **Fix:**
  - Open a terminal into your docker container:
    ```shell
    docker exec -it <container_id> bash
    ```
    Then remove any corrupted files under `~/.cache/torch/hub/checkpoints/`. It will re-download automatically next time a job runs.
  - To avoid repeated downloads, you can also mount the cache directory onto the host using the `-v` flag on your `docker run` command.
    For details about mounting volumes, please consult the official Docker documentation.

### Container killed or crashes under load
- **Symptom:** Server stops or computer restarts during job processing
- **Fix:**  
  - Monitor system RAM and temperatures, and check for overheating or insufficient resources.  
  - Try lowering `-cpu-workers` to `1` or `0` in the `docker run` command.

### “out of shm” error
- **Symptom:** Job fails with “out of shared memory.”  
- **Fix:**  
  - Ensure you run Docker with `--shm-size 512m` (already included in the example command).  

### ngrok URL not working, or changes every time you start it
- **Symptom:** Public jobs don’t reach your server when using ngrok.  
- **Fix:**  
  - Use `--url` with a **static ngrok domain** (set up via the ngrok dashboard).  
  - Ensure ngrok is always running; if your server restarts, you might need to also start ngrok again.

### Docker crashes on Windows
- **Symptom:** Container stops after a few minutes on Windows.
- **Fix:**  
  - Restart Docker Desktop.
  - If the issue persists, also restart your computer.

---

💡 **Still stuck?**  
If your issue remains, please:  
1. Check `docker logs <container_id>` for error messages.
2. Share logs and system specs with the [Auki Labs](https://www.aukilabs.com) team for support.
