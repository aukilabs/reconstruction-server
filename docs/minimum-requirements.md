# Minimum Requirements

Most Windows or Linux machines with modern NVIDIA GPUs will be able to run a Reconstruction Server. We have tested on desktops, laptops, and various virtual machines.

**NOTE:** The requirements below are high to ensure smooth processing. If you successfully run on lower hardware specs, please let us know!

- **OS (64-bit):** Windows 10 / 11 or Ubuntu 22.04 / 24.04 LTS
- **CPU:** 8 cores
- **RAM:** 12 GiB
- **GPU:** Nvidia with 8+ GiB VRAM. Tested on RTX 3090, RTX 4060 and T4. May work on slightly older Nvidia cards too with enough VRAM and recent CUDA. Blackwell GPUs (RTX 50xx) are not supported in the current version.
- **NVIDIA driver:** recent version that supports CUDA 12.x
- **CUDA toolkit:** CUDA 12.x
- **Disk space:** 20 GB or more
- **Docker**

**Additionally**, you need these in order to expose the Reconstruction Server to the Internet:

- A stable Internet connection with
  - an externally accessible local port (TCP)
  - static and public IP address with port forwarding
  - at least 10 Mbps downstream and upstream
- Optionally, a domain name configured to point to your IP address
- A [dynamic DNS service](https://en.wikipedia.org/wiki/Dynamic_DNS) if you don't have a static IP address

See [Deployment](deployment.md) for more information.
