#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

JETSON="nvidia1@192.168.1.201"
REMOTE_DIR="~/fw-edge"

echo "Syncing..."
rsync -avz \
    --exclude='.git' \
    --filter=':- .gitignore' \
    --temp-dir=/tmp/ \
    "$SCRIPT_DIR/" $JETSON:$REMOTE_DIR

# 2. Run remotely
# echo "Running..."
# ssh -t $JETSON "docker exec -t jetson_dev python3 /app/main.py"
