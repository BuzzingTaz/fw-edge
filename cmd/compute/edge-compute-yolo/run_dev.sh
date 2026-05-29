#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

JETSON="nvidia3@10.2.140.212"
REMOTE_DIR="~/Documents/fw-edge-wip/cmd/compute/edge-compute-yolo"

echo "Syncing..."
rsync -avz \
    --exclude='.git' \
    --filter=':- .gitignore' \
    --temp-dir=/tmp/ \
    "$SCRIPT_DIR/" $JETSON:$REMOTE_DIR #push the data
    #  $JETSON:$REMOTE_DIR/ "$SCRIPT_DIR/" #pull the data

# 2. Run remotely
# echo "Running..."
# ssh -t $JETSON "docker exec -t jetson_dev python3 /app/main.py"
