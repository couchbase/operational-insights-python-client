#!/bin/bash

# USAGE: Run this script from anywhere.
# It generates the gRPC stubs for the protos vendored in performer/proto into
# performer/insights_performer/protocol.  The stubs are not committed; the
# Dockerfile runs this script as part of the image build.

set -e

PERFORMER_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PERFORMER_DIR"

PROTO_DIR=proto
OUT_DIR=insights_performer/protocol
PYTHON="${UV_PYTHON:-python}"

mkdir -p "$OUT_DIR"

$PYTHON -m grpc_tools.protoc \
    --proto_path "$PROTO_DIR/columnar" \
    --proto_path "$PROTO_DIR" \
    --python_out="$OUT_DIR" \
    --pyi_out="$OUT_DIR" \
    "$PROTO_DIR"/columnar/*.proto "$PROTO_DIR"/shared.echo.proto

$PYTHON -m grpc_tools.protoc \
    --proto_path "$PROTO_DIR/columnar" \
    --proto_path "$PROTO_DIR" \
    --python_out="$OUT_DIR" \
    --pyi_out="$OUT_DIR" \
    --grpc_python_out="$OUT_DIR" \
    "$PROTO_DIR"/columnar/columnar.services.proto
