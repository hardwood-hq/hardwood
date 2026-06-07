#!/bin/bash
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -e

# Build CLI Docker image from locally built hardwood CLI binary and completion script.
# By default, uses an existing built binary. Use -f/--force to rebuild the CLI.
#
# Usage: cd cli && ./build-cli-docker.sh [options] [image-tag]
#
# Options:
#   -f, --force         Force rebuild of the CLI binary (containerized native build)
#
# Examples:
#   cd cli && ./build-cli-docker.sh                    # uses existing binary, builds as hardwood/hardwood:local
#   cd cli && ./build-cli-docker.sh v1.0.0             # uses existing binary, builds as hardwood/hardwood:v1.0.0
#   cd cli && ./build-cli-docker.sh -f                 # forces rebuild, builds as hardwood/hardwood:local
#   cd cli && ./build-cli-docker.sh --force v1.0.0     # forces rebuild, builds as hardwood/hardwood:v1.0.0

FORCE_REBUILD=false
IMAGE_TAG="local"

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -f|--force)
      FORCE_REBUILD=true
      shift
      ;;
    *)
      IMAGE_TAG="$1"
      shift
      ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE_NAME="hardwood/hardwood:${IMAGE_TAG}"

echo "Building Docker image for hardwood CLI: $IMAGE_NAME"
echo ""

# Build CLI binary only if it doesn't exist or if --force is specified
if [ ! -f "$REPO_ROOT/cli/target/hardwood-cli" ] || [ "$FORCE_REBUILD" = true ]; then
  echo "Building containerized Linux native binary..."
  echo "This requires Docker to be running."
  echo ""
  cd "$REPO_ROOT"
  ./mvnw -Dnative -Dquarkus.native.container-build=true package -pl cli -am
  cd "$REPO_ROOT/cli"
  echo ""
else
  echo "Using existing CLI binary at cli/target/hardwood-cli"
  echo "(Use -f/--force to rebuild)"
  echo ""
fi

# Check if the completion script exists
if [ ! -f "$REPO_ROOT/cli/target/hardwood_completion" ]; then
  echo "Error: Completion script not found at cli/target/hardwood_completion"
  echo "Re-run the build or check the build output."
  exit 1
fi

echo ""
echo "Copying binary and completion script to docker build context..."
cp "$REPO_ROOT/cli/target/hardwood-cli" "$REPO_ROOT/cli/docker/hardwood"
cp "$REPO_ROOT/cli/target/hardwood_completion" "$REPO_ROOT/cli/docker/hardwood_completion"
chmod +x "$REPO_ROOT/cli/docker/hardwood"

echo "Building Docker image..."
docker build -t "$IMAGE_NAME" -f "$REPO_ROOT/cli/docker/Dockerfile" "$REPO_ROOT/cli/docker"

echo ""
echo "✓ Docker image built successfully: $IMAGE_NAME"
echo ""
echo "Run the image with:"
echo "  docker run --rm $IMAGE_NAME --help"
echo "  docker run --rm -v \"\$(pwd)\":/repo -w /repo $IMAGE_NAME info -f core/src/test/resources/plain_uncompressed.parquet"

