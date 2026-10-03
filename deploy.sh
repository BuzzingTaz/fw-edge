#!/bin/bash
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$SCRIPT_DIR"

# Default configuration (can be overridden by env vars)
JETSON_HOST=${JETSON_HOST:-"nvidia1@10.42.0.229"}
REMOTE_DIR="~/fw-edge"

if [ -z "$1" ]; then
    echo "Usage: ./deploy.sh [host|jetson]"
    exit 1
fi

TARGET=$1

echo "Step 1: Generating Protobufs..."
"$PROJECT_ROOT/scripts/generate_protos.sh"

if [ "$TARGET" = "host" ]; then
    echo "Step 2: Deploying host services..."
    docker compose up -d --build

elif [ "$TARGET" = "jetson" ]; then
    echo "Step 2: Syncing required files to Jetson ($JETSON_HOST)..."

    # We only sync necessary files for the Jetson runtime, preserving directory structure
    rsync -avz --temp-dir=/tmp/ \
        --include='cmd/' \
        --include='cmd/***' \
        --include='internal/***' \
        --include='pkg/***' \
        --include='go.mod' \
        --include='go.sum' \
        --include='*.go' \
        --include='cmd/compute2/***' \
        --include='cmd/compute3/***' \
        --include='proto/***' \
        --include='Dockerfile.jetson' \
        --include='Dockerfile' \
        --include='scripts/***' \
        --include='compose.jetson.yaml' \
        --include='yolo*.engine' \
        --include='yolo*.pt' \
        --include='*.yaml' \
        --exclude='*' \
        "$PROJECT_ROOT/" "$JETSON_HOST:$REMOTE_DIR"

    # echo "Step 3: Deploying on Jetson..."
    # # Build the image and start the compose stack
    # ssh -t $JETSON_HOST "cd $REMOTE_DIR && docker compose -f compose.jetson.yaml up -d --build compute2"
    #
    # echo "Deployment to Jetson complete."
else
    echo "Invalid target: $TARGET"
    echo "Usage: ./deploy.sh [host|jetson]"
    exit 1
fi
