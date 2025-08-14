# Minimum Requirements

Most Windows or Linux machines with modern NVIDIA GPUs will be able to run a Reconstruction Server. We have tested on desktops, laptops, and various virtual machines.

- **OS (64-bit):** Windows 10 / 11 or Ubuntu 22.04 / 24.04 LTS
- **CPU:** 4 cores
- **RAM:** 4 GiB
- **GPU:** TBD during closed beta; preferably NVIDIA T4 or better
- **GPU memory:** 3 GiB VRAM
- **NVIDIA driver:** recent version that supports CUDA 12.x
- **CUDA toolkit:** CUDA 12.x
- **Disk space:** 10 GB
- **Docker**

Additionally, you need these in order to expose the Reconstruction Server to the Internet:

- A stable Internet connection with
  - an externally accessible local port (TCP)
  - static and public IP address with port forwarding
  - at least 10 Mbps downstream and upstream
- A domain name configured to point to your IP address
- A [dynamic DNS service](https://en.wikipedia.org/wiki/Dynamic_DNS) if you don't have a static IP address

See
[Deployment](deployment.md) for more information.
