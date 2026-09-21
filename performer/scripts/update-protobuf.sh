#!/bin/bash

# USAGE: Run this script from anywhere.
# It fetches the latest proto files from the `couchbaselabs/fit-protocol` repo,
# copies the ones this performer needs (the `columnar/` service definitions and
# the shared `echo` service) into performer/proto, and leaves them staged for
# manual commit.

set -e

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

DEST_DIR=performer/proto
TMP_DIR=$(mktemp -d)

trap 'rm -rf "$TMP_DIR"' EXIT

curl --location --fail https://github.com/couchbaselabs/fit-protocol/archive/refs/heads/main.zip -o "$TMP_DIR/fit-protocol.zip"
unzip -q "$TMP_DIR/fit-protocol.zip" -d "$TMP_DIR/unzipped"

SRC_DIR="$TMP_DIR/unzipped/fit-protocol-main"
[ -d "$SRC_DIR" ] || { echo "ERROR: Missing expected directory: $SRC_DIR" >&2; exit 1; }

rm -rf "$DEST_DIR/columnar"
mkdir -p "$DEST_DIR/columnar"
cp -R "$SRC_DIR/columnar"/. "$DEST_DIR/columnar"
cp "$SRC_DIR/operational/shared.echo.proto" "$DEST_DIR/shared.echo.proto"

git add --all "$DEST_DIR"

echo
echo "Protobuf update complete! Please manually commit any modified files."
echo
