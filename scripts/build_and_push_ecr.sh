#!/usr/bin/env bash

# Build and push Docker image to AWS ECR similar to .github/workflows/ci-rust.yml
# - Logs into ECR using AWS profile `auki-common` by default
# - Tags the image with the current commit SHA
# - If on branch `main`, also tags and pushes `latest`
#
# Environment overrides (optional):
#   AWS_PROFILE   - defaults to "auki-common"
#   AWS_REGION    - defaults to "us-east-1"
#   REGISTRY      - defaults to "026987513085.dkr.ecr.${AWS_REGION}.amazonaws.com"
#   REPO          - defaults to "reconstruction-server"
#   DOCKERFILE    - defaults to "docker/Dockerfile"
#   CONTEXT       - defaults to current repo root (".")
#   IMAGE_TAG     - defaults to `git rev-parse HEAD`

set -euo pipefail

echo "==> Preparing ECR login and Docker build (local)"

PROFILE="${AWS_PROFILE:-auki-common}"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID_DEFAULT="026987513085"
REGISTRY_DEFAULT="${ACCOUNT_ID_DEFAULT}.dkr.ecr.${REGION}.amazonaws.com"
REGISTRY="${REGISTRY:-${REGISTRY_DEFAULT}}"
REPO="${REPO:-reconstruction-server}"
DOCKERFILE="${DOCKERFILE:-docker/Dockerfile}"
CONTEXT="${CONTEXT:-.}"

# Resolve git info if not provided
IMAGE_TAG="${IMAGE_TAG:-}"
if [[ -z "${IMAGE_TAG}" ]]; then
  if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    IMAGE_TAG="$(git rev-parse --verify HEAD)"
  else
    echo "ERROR: Not a git repository and IMAGE_TAG not provided" >&2
    exit 1
  fi
fi

BRANCH="${BRANCH:-}"
if [[ -z "${BRANCH}" ]]; then
  if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    BRANCH="$(git rev-parse --abbrev-ref HEAD)"
  else
    BRANCH="unknown"
  fi
fi

echo "AWS_PROFILE=${PROFILE}"
echo "AWS_REGION=${REGION}"
echo "REGISTRY=${REGISTRY}"
echo "REPO=${REPO}"
echo "DOCKERFILE=${DOCKERFILE}"
echo "CONTEXT=${CONTEXT}"
echo "IMAGE_TAG=${IMAGE_TAG}"
echo "BRANCH=${BRANCH}"

echo "==> Logging in to AWS ECR: ${REGISTRY}"
AWS_PROFILE="${PROFILE}" aws ecr get-login-password --region "${REGION}" \
  | docker login --username AWS --password-stdin "${REGISTRY}"

IMAGE_COMMIT="${REGISTRY}/${REPO}:${IMAGE_TAG}"
IMAGE_LATEST="${REGISTRY}/${REPO}:latest"

echo "==> Ensuring docker buildx is available"
if ! docker buildx version >/dev/null 2>&1; then
  echo "ERROR: docker buildx not found. Please install Docker Buildx (Docker 20.10+)." >&2
  exit 1
fi

# Ensure there is an active builder; create one if needed
if ! docker buildx ls | grep -q '\*'; then
  echo "==> Creating and bootstrapping a buildx builder"
  docker buildx create --name ecrbuilder --use >/dev/null
fi
docker buildx inspect --bootstrap >/dev/null

if [[ "${BRANCH}" == "main" ]]; then
  echo "==> Building and pushing with tags: ${IMAGE_COMMIT}, ${IMAGE_LATEST}"
  docker buildx build \
    -t "${IMAGE_COMMIT}" \
    -t "${IMAGE_LATEST}" \
    --platform linux/amd64 \
    --push \
    -f "${DOCKERFILE}" \
    "${CONTEXT}"
else
  echo "==> Building and pushing with tag: ${IMAGE_COMMIT}"
  docker buildx build \
    -t "${IMAGE_COMMIT}" \
    --platform linux/amd64 \
    --push \
    -f "${DOCKERFILE}" \
    "${CONTEXT}"
fi

echo "==> Done."
