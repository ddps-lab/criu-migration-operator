#!/bin/bash
# Copy CRIU binaries from system install path to Docker build context.
# Run 'make install' in criu-s3 first, then run this script.
# These binaries are excluded from git (see .gitignore).
#
# Usage: ./copy-criu.sh [CRIU_INSTALL_DIR]
#   Default: /usr/local/sbin

set -e

CRIU_INSTALL_DIR="${1:-/usr/local/sbin}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

CRIU_BIN="${CRIU_INSTALL_DIR}/criu"
CRIU_NS_BIN="${CRIU_INSTALL_DIR}/criu-ns"

if [ ! -f "$CRIU_BIN" ]; then
    echo "Error: CRIU binary not found at $CRIU_BIN"
    echo "Run 'make install' in criu-s3 build directory first."
    exit 1
fi

cp "$CRIU_BIN" "${SCRIPT_DIR}/criu-binary"
cp "$CRIU_NS_BIN" "${SCRIPT_DIR}/criu-ns-binary"
chmod +x "${SCRIPT_DIR}/criu-binary" "${SCRIPT_DIR}/criu-ns-binary"

echo "Copied CRIU binaries to build context:"
echo "  criu-binary    <- $CRIU_BIN"
echo "  criu-ns-binary <- $CRIU_NS_BIN"
"${SCRIPT_DIR}/criu-binary" --version 2>&1 || true
