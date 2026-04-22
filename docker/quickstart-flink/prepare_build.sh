#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

FLINK_VERSION="${FLINK_VERSION:-1.20}"
FLUSS_VERSION="${FLUSS_VERSION:-}"
FLUSS_MAVEN_REPO_URL="${FLUSS_MAVEN_REPO_URL:-https://repo1.maven.org/maven2}"
IMAGE_NAME="${IMAGE_NAME:-}"
PLATFORMS="${PLATFORMS:-linux/arm64/v8,linux/amd64}"
PUSH_IMAGE=false

usage() {
    cat <<'EOF'
Usage: ./prepare_build.sh --fluss-version <version> [options]

Options:
  --fluss-version <version>  Required. Fluss version used for bundled jars.
  --flink-version <version>  Flink base image version. Default: 1.20
  --repo-url <url>           Fluss Maven repository URL.
  --image-name <name>        Full output image name. Default:
                             apache/fluss-quickstart-flink:<flink-version>-<fluss-version>
  --platforms <value>        docker buildx platforms list.
  --push                     Add --push to docker buildx build.
  -h, --help                 Show this help message.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --fluss-version)
            FLUSS_VERSION="$2"
            shift 2
            ;;
        --flink-version)
            FLINK_VERSION="$2"
            shift 2
            ;;
        --repo-url)
            FLUSS_MAVEN_REPO_URL="$2"
            shift 2
            ;;
        --image-name)
            IMAGE_NAME="$2"
            shift 2
            ;;
        --platforms)
            PLATFORMS="$2"
            shift 2
            ;;
        --push)
            PUSH_IMAGE=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage
            exit 1
            ;;
    esac
done

if [[ -z "${FLUSS_VERSION}" ]]; then
    echo "FLUSS_VERSION is required." >&2
    usage
    exit 1
fi

if [[ -z "${IMAGE_NAME}" ]]; then
    IMAGE_NAME="apache/fluss-quickstart-flink:${FLINK_VERSION}-${FLUSS_VERSION}"
fi

BUILD_CMD=(
    docker buildx build
    --platform "${PLATFORMS}"
    --build-arg "FLINK_VERSION=${FLINK_VERSION}"
    --build-arg "FLUSS_VERSION=${FLUSS_VERSION}"
    --build-arg "FLUSS_MAVEN_REPO_URL=${FLUSS_MAVEN_REPO_URL}"
    --tag "${IMAGE_NAME}"
)

if [[ "${PUSH_IMAGE}" == "true" ]]; then
    BUILD_CMD+=(--push)
fi

BUILD_CMD+=("${SCRIPT_DIR}")

printf 'Prepared command:\n\n'
printf '%q ' "${BUILD_CMD[@]}"
printf '\n'
