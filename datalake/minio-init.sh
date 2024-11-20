#!/bin/bash

echo "Waiting for Minio to be ready..."
wait-for-it $MINIO_HOST:9000 -- echo "Minio is up"

mc alias set minio http://$MINIO_HOST:9000 $MINIO_ACCESS_KEY $MINIO_SECRET_KEY


mc mb minio/raw --ignore-existing
mc mb minio/bronze --ignore-existing
mc mb minio/silver --ignore-existing
mc mb minio/gold --ignore-existing

