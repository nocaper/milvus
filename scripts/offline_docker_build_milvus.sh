#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/offline_docker_build_milvus.sh prepare [bundle_dir]
  scripts/offline_docker_build_milvus.sh build [bundle_dir]

prepare:
  Run on a machine that can access Docker Hub / Milvus registry / Go / Conan /
  Rust networks. It pulls the Milvus builder image, prewarms build caches, and
  writes a transferable bundle.

build:
  Run on the restricted company machine. It loads the builder image/cache from
  the bundle if present, then builds the current working tree into bin/milvus.

Environment:
  BUILDER_IMAGE  Full builder image override. When unset, the script uses
                 ${IMAGE_REPO}/milvus-env:${OS_NAME}-${DATE_VERSION}.
  IMAGE_ARCH     Builder/container architecture. Default from .env, usually
                 amd64; set to arm64 for ARM machines.
  OS_NAME        Milvus-supported builder OS name. This repository provides
                 ubuntu20.04, ubuntu22.04, ubuntu24.04, rockylinux9, and
                 amazonlinux2023 builder Dockerfiles. There is no openEuler
                 builder Dockerfile in this tree.
  DOCKER_PLATFORM Optional docker platform, for example linux/arm64.
  SKIP_DOCKER_PULL Set to 1 when BUILDER_IMAGE already exists locally and
                   should not be pulled.
  PREPARE_CMD  Command used during prepare cache warmup.
               Default: jobs=$(nproc) make milvus
  BUILD_CMD    Command used during build.
               Default: jobs=${JOBS:-$(nproc)} make milvus
  JOBS         Optional build parallelism for build mode.
EOF
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: command not found: $1" >&2
    exit 1
  fi
}

repo_root() {
  local source="${BASH_SOURCE[0]}"
  while [ -h "$source" ]; do
    local dir
    dir="$(cd -P "$(dirname "$source")" && pwd)"
    source="$(readlink "$source")"
    [[ "$source" != /* ]] && source="$dir/$source"
  done
  cd -P "$(dirname "$source")/.." && pwd
}

load_env() {
  local env_image_repo="${IMAGE_REPO-}"
  local env_image_arch="${IMAGE_ARCH-}"
  local env_os_name="${OS_NAME-}"
  local env_date_version="${DATE_VERSION-}"
  local env_builder_image="${BUILDER_IMAGE-}"
  local env_builder_key="${BUILDER_KEY-}"

  if [ -f ".env" ]; then
    set -a
    # shellcheck disable=SC1091
    . ./.env
    set +a
  fi

  [ -n "$env_image_repo" ] && IMAGE_REPO="$env_image_repo"
  [ -n "$env_image_arch" ] && IMAGE_ARCH="$env_image_arch"
  [ -n "$env_os_name" ] && OS_NAME="$env_os_name"
  [ -n "$env_date_version" ] && DATE_VERSION="$env_date_version"
  [ -n "$env_builder_image" ] && BUILDER_IMAGE="$env_builder_image"
  [ -n "$env_builder_key" ] && BUILDER_KEY="$env_builder_key"

  : "${IMAGE_REPO:=milvusdb}"
  : "${IMAGE_ARCH:=amd64}"
  : "${OS_NAME:=ubuntu22.04}"

  if [ -z "${BUILDER_IMAGE:-}" ] && [ -z "${DATE_VERSION:-}" ]; then
    echo "ERROR: DATE_VERSION is not set and .env was not found or incomplete." >&2
    exit 1
  fi

  : "${BUILDER_IMAGE:=${IMAGE_REPO}/milvus-env:${OS_NAME}-${DATE_VERSION}}"
  : "${BUILDER_KEY:=${IMAGE_ARCH}-${OS_NAME}}"
}

cache_root() {
  echo ".docker/offline-${BUILDER_KEY}"
}

platform_args() {
  if [ -n "${DOCKER_PLATFORM:-}" ]; then
    echo "--platform ${DOCKER_PLATFORM}"
  fi
}

docker_run_builder() {
  local cmd="$1"
  local cache
  cache="$(cache_root)"

  mkdir -p \
    "${cache}/go-mod" \
    "${cache}/conan2" \
    "${cache}/ccache" \
    "${cache}/cargo-registry" \
    "${cache}/cargo-git" \
    "${cache}/vscode-extensions"

  # shellcheck disable=SC2046
  docker run --rm $(platform_args) --shm-size=2g \
    -v "$PWD":/go/src/github.com/milvus-io/milvus \
    -v "$PWD/${cache}/go-mod":/go/pkg/mod \
    -v "$PWD/${cache}/conan2":/home/milvus/.conan2 \
    -v "$PWD/${cache}/ccache":/ccache \
    -v "$PWD/${cache}/cargo-registry":/root/.cargo/registry \
    -v "$PWD/${cache}/cargo-git":/root/.cargo/git \
    -v "$PWD/${cache}/vscode-extensions":/home/milvus/.vscode-server/extensions \
    -w /go/src/github.com/milvus-io/milvus \
    -e GO111MODULE=on \
    -e CONAN_HOME=/home/milvus/.conan2 \
    -e CCACHE_DIR=/ccache \
    -e USE_ASAN="${USE_ASAN:-OFF}" \
    "$BUILDER_IMAGE" \
    bash -lc "$cmd"
}

prepare() {
  local bundle_dir="${1:-offline_milvus_build_bundle}"
  local cache
  cache="$(cache_root)"

  require_cmd docker
  require_cmd tar
  require_cmd gzip

  mkdir -p "$bundle_dir"

  echo "Builder image: $BUILDER_IMAGE"
  if [ "${SKIP_DOCKER_PULL:-0}" = "1" ]; then
    if ! docker image inspect "$BUILDER_IMAGE" >/dev/null 2>&1; then
      echo "ERROR: SKIP_DOCKER_PULL=1 but image does not exist locally: $BUILDER_IMAGE" >&2
      exit 1
    fi
  else
    # shellcheck disable=SC2046
    docker pull $(platform_args) "$BUILDER_IMAGE"
  fi

  local prepare_cmd="${PREPARE_CMD:-jobs=\$(nproc) make milvus}"
  echo "Preparing caches with: $prepare_cmd"
  docker_run_builder "$prepare_cmd"

  local image_tar="${bundle_dir}/milvus-builder-${BUILDER_KEY}.tar"
  echo "Saving builder image to: ${image_tar}.gz"
  docker save -o "$image_tar" "$BUILDER_IMAGE"
  gzip -f "$image_tar"

  echo "Packing build caches to: ${bundle_dir}/milvus-build-cache-${BUILDER_KEY}.tar.gz"
  tar -czf "${bundle_dir}/milvus-build-cache-${BUILDER_KEY}.tar.gz" -C "$cache" .

  cat >"${bundle_dir}/README.txt" <<EOF
Builder image: ${BUILDER_IMAGE}
Cache source: ${cache}

On the restricted machine:
  cd milvus
  tar -xzf /path/to/offline_milvus_build_bundle/milvus-build-cache-${BUILDER_KEY}.tar.gz -C $(cache_root)
  gzip -dc /path/to/offline_milvus_build_bundle/milvus-builder-${BUILDER_KEY}.tar.gz | docker load
  scripts/offline_docker_build_milvus.sh build /path/to/offline_milvus_build_bundle
EOF

  echo "Done. Transfer this directory to the restricted machine: $bundle_dir"
}

build() {
  local bundle_dir="${1:-offline_milvus_build_bundle}"
  local cache
  cache="$(cache_root)"

  require_cmd docker
  require_cmd tar

  local image_gz="${bundle_dir}/milvus-builder-${BUILDER_KEY}.tar.gz"
  local cache_tgz="${bundle_dir}/milvus-build-cache-${BUILDER_KEY}.tar.gz"

  if ! docker image inspect "$BUILDER_IMAGE" >/dev/null 2>&1; then
    if [ -f "$image_gz" ]; then
      require_cmd gzip
      echo "Loading builder image from: $image_gz"
      gzip -dc "$image_gz" | docker load
    else
      echo "ERROR: builder image not available: $BUILDER_IMAGE" >&2
      echo "Expected image bundle: $image_gz" >&2
      exit 1
    fi
  fi

  mkdir -p "$cache"
  if [ -f "$cache_tgz" ]; then
    echo "Extracting cache bundle: $cache_tgz"
    tar -xzf "$cache_tgz" -C "$cache"
  else
    echo "WARN: cache bundle not found: $cache_tgz"
    echo "      Build may try to access the network."
  fi

  local build_cmd="${BUILD_CMD:-jobs=\${JOBS:-\$(nproc)} make milvus}"
  echo "Building current working tree with: $build_cmd"
  docker_run_builder "$build_cmd"
}

main() {
  local root
  root="$(repo_root)"
  cd "$root"
  load_env

  case "${1:-}" in
    prepare)
      prepare "${2:-offline_milvus_build_bundle}"
      ;;
    build)
      build "${2:-offline_milvus_build_bundle}"
      ;;
    -h|--help|help|"")
      usage
      ;;
    *)
      echo "ERROR: unknown command: $1" >&2
      usage
      exit 1
      ;;
  esac
}

main "$@"
