#!/bin/bash

echo "Cleaning previous build..."
rm -f dfs-client

echo "Building the client..."
go build -o dfs-client cmd/client/main.go

# Check if build was successful
if [ $? -ne 0 ]; then
    echo "Build failed. Exiting."
    exit 1
fi
echo "Build successful."

echo "Starting the client..."
./dfs-client

# Cleanup after client exits
echo "Cleaning up..."
rm -f dfs-client

echo "Cleanup complete. Exiting."
