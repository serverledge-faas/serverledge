#!/bin/sh
set -e

for f in ./*.zip; do
    [ -e "$f" ] || continue
    unzip -o -- "$f"
done



ARCH=$(uname -m) # architecture of node executing the function

# select the correct binary
if [ "$ARCH" = "x86_64" ]; then
    BINARY="./handler_amd64"
elif [ "$ARCH" = "aarch64" ]; then
    BINARY="./handler_arm64"
else
    echo "Error: Unsupported architecture '$ARCH', or wrong names for executables"
    echo "Usage: zip file with both: handler_amd64 and handler_arm64 executable files"
fi

if [ -f "$BINARY" ]; then
    chmod +x "$BINARY"
    exec "$BINARY"
else
    echo "Error: Binary '$BINARY' not found in the uploaded package."
    echo "Files available:"
    ls -l
    exit 1
fi