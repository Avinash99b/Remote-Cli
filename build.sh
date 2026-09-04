#!/usr/bin/env bash
set -e

# Script to build standalone binaries for remote-cli, remote-cli-server, and client.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Building Remote Client Manager Binaries ==="

# Check if PyInstaller is installed
if ! command -v pyinstaller &> /dev/null && ! python3 -m PyInstaller --version &> /dev/null; then
    echo "PyInstaller not found. Installing pyinstaller..."
    python3 -m pip install pyinstaller
fi

# Determine pyinstaller command
if command -v pyinstaller &> /dev/null; then
    PYINSTALLER_CMD="pyinstaller"
else
    PYINSTALLER_CMD="python3 -m PyInstaller"
fi

echo "Building remote-cli (from manager.py)..."
$PYINSTALLER_CMD --clean --onefile --name remote-cli manager.py

echo "Building remote-cli-server (from server.py)..."
$PYINSTALLER_CMD --clean --onefile --name remote-cli-server server.py

echo "Building client (from client.py)..."
$PYINSTALLER_CMD --clean --onefile --name client client.py

echo "Copying binaries to repository root..."
cp dist/remote-cli ./remote-cli
cp dist/remote-cli-server ./remote-cli-server
cp dist/client ./client

echo "=== Build Complete ==="
echo "Output binaries:"
echo "  1. ./remote-cli (also in ./dist/remote-cli)"
echo "  2. ./remote-cli-server (also in ./dist/remote-cli-server)"
echo "  3. ./client (also in ./dist/client)"
