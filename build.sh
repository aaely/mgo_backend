#!/bin/bash
set -e

IMAGE=${1:-mgo-backend:latest}

echo "Building React frontend..."
cd ../lms_react
npm run build

echo "Copying dist to backend..."
cp -r dist ../mgo_backend/dist

echo "Building Docker image: $IMAGE"
cd ../mgo_backend
docker build -t "$IMAGE" .

echo "Done: $IMAGE"
