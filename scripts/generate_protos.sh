#!/bin/bash
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Building protoc builder image..."
docker build -t fw-protoc-builder -f "$SCRIPT_DIR/Dockerfile.protoc" "$SCRIPT_DIR"

echo "Generating protobufs for Go and Python..."
docker run --rm -v "$PROJECT_ROOT":/app fw-protoc-builder bash -c '
# Go generation
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       ./proto/fw.proto ./proto/engine.proto

# Python generation
/venv/bin/python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. ./proto/fw.proto ./proto/engine.proto
'

echo "Done."
