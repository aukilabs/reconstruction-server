#!/bin/bash
set -euo pipefail

MIN_SHM_KB=$((500 * 1024))

shm_kb="$(df -kP /dev/shm | awk 'NR==2 {print $2}')"
if [ -z "${shm_kb}" ] || [ "${shm_kb}" -lt "${MIN_SHM_KB}" ]; then
  echo "ERROR: /dev/shm is too small (${shm_kb:-unknown} KB configured; need at least ${MIN_SHM_KB} KB)." >&2
  echo "Restart the container with --shm-size 512m (see docs/deployment.md)." >&2
  exit 1
fi

if ! python3 -c "import sys; import torch; sys.exit(0 if torch.cuda.is_available() else 1)"; then
  echo "ERROR: CUDA is not available inside this container." >&2
  echo "Ensure Docker was started with --gpus all, verify the host GPU with nvidia-smi," >&2
  echo "and see docs/deployment.md (Troubleshooting: GPU not detected)." >&2
  exit 1
fi

exec /app/compute-node "$@"
