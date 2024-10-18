#!/bin/bash

echo "Starting DFS services..."
./start_dfs.sh

echo "Cleaning previous build..."
rm -f dfs-client

echo "Building the client..."
go build -o dfs-client cmd/client/main.go

if [ $? -ne 0 ]; then
    echo "Build failed. Exiting."
    exit 1
fi
echo "Build successful."

echo "Starting the client..."
./dfs-client

echo "Cleaning up..."
rm -f dfs-client

echo "Stopping DFS services..."
sudo docker-compose down
echo "Cleanup complete. Exiting."
