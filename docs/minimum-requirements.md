# Minimum Requirements

Most Windows or Linux machines with modern NVIDIA GPUs will be able to run a Reconstruction Server. We have tested on desktops, laptops, and various virtual machines.

**NOTE:** The requirements below are high to ensure smooth processing. If you successfully run on lower hardware specs, please let us know!

- **OS (64-bit):** Windows 10 / 11 or Ubuntu 22.04 / 24.04 LTS
- **CPU:** 8 cores
- **RAM:** 12 GiB
- **GPU:** Nvidia with 8+ GiB VRAM. Tested on RTX 3090, RTX 4060, RTX 5070 Ti and T4. May work on older Nvidia cards too with enough VRAM and recent CUDA.
- **NVIDIA driver:** recent version that supports CUDA 12.8
- **Disk space:** 40 GB or more
- **Docker** _- Windows support tested with Docker Desktop and WSL 2_
- A stable Internet connection with at least 10 Mbps downstream and upstream

See [Deployment](deployment.md) for more information.
