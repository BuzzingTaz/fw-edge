#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

JETSON="nvidia3@10.42.0.81"
REMOTE_DIR="/home/nvidia3/Documents/fw-edge-wip/
"

echo "Syncing..."
rsync -avz \
    --exclude='.git' \
    --filter=':- .gitignore' \
    --temp-dir=/tmp/ \
    "$SCRIPT_DIR/" $JETSON:$REMOTE_DIR

# 2. Run remotely
# echo "Running..."
# ssh -t $JETSON "docker exec -t jetson_dev python3 /app/main.py"
