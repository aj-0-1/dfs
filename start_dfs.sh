#!/bin/bash

echo "Building and starting DFS containers..."
sudo docker-compose up --build -d

echo "Waiting for services to be ready..."
sleep 10

echo "DFS services are now running."
