#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/setup_milvus_proto_offline.sh <milvus-proto-dir-or-tar.gz>

This installs a pre-downloaded milvus-proto checkout into:
  cmake_build/thirdparty/milvus-proto

After this succeeds, build with:
  MILVUS_PROTO_OFFLINE=1 jobs=$(nproc) make milvus
EOF
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

expected_proto_commit() {
  local api_version
  api_version=$(awk '$1 == "github.com/milvus-io/milvus-proto/go-api/v3" { print $2; exit }' go.mod)
  echo "$api_version" | awk -F'-' '{print $3}'
}

find_proto_source_dir() {
  local root="$1"

  if [ -d "$root/proto" ]; then
    echo "$root"
    return 0
  fi

  local candidate
  candidate=$(find "$root" -maxdepth 3 -type d -name proto -print -quit 2>/dev/null || true)
  if [ -n "$candidate" ]; then
    dirname "$candidate"
    return 0
  fi

  return 1
}

main() {
  if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ] || [ $# -ne 1 ]; then
    usage
    exit 1
  fi

  local root
  root="$(repo_root)"
  cd "$root"

  local input="$1"
  local work_dir=""
  local source_dir=""

  if [ -d "$input" ]; then
    source_dir="$(find_proto_source_dir "$input")" || {
      echo "ERROR: cannot find proto directory under: $input" >&2
      exit 1
    }
  elif [ -f "$input" ]; then
    mkdir -p "${root}/cmake_build"
    work_dir="$(mktemp -d "${root}/cmake_build/.milvus-proto-offline.XXXXXX")"
    tar -xzf "$input" -C "$work_dir"
    source_dir="$(find_proto_source_dir "$work_dir")" || {
      echo "ERROR: cannot find proto directory in archive: $input" >&2
      exit 1
    }
  else
    echo "ERROR: input does not exist: $input" >&2
    exit 1
  fi

  local expected_commit
  expected_commit="$(expected_proto_commit)"
  if [ -n "$expected_commit" ] && [ -d "$source_dir/.git" ]; then
    local actual_commit
    actual_commit="$(git -C "$source_dir" rev-parse --short=12 HEAD 2>/dev/null || true)"
    if [ -n "$actual_commit" ] && [ "$actual_commit" != "$expected_commit" ]; then
      echo "WARN: milvus-proto checkout is $actual_commit, expected $expected_commit"
      echo "      Continue only if you intentionally use this proto version."
    fi
  fi

  local target="cmake_build/thirdparty/milvus-proto"
  mkdir -p "cmake_build/thirdparty"

  if [ -e "$target" ]; then
    local backup="${target}.bak.$(date +%Y%m%d%H%M%S)"
    echo "Existing $target found, moving it to $backup"
    mv "$target" "$backup"
  fi

  mkdir -p "$target"
  cp -a "$source_dir"/. "$target"/

  if [ -n "$work_dir" ]; then
    rm -rf "$work_dir"
  fi

  echo "Installed milvus-proto into $target"
  echo "Expected proto commit from go.mod: ${expected_commit:-unknown}"
  echo ""
  echo "Build command:"
  echo "  MILVUS_PROTO_OFFLINE=1 jobs=\$(nproc) make milvus"
}

main "$@"
