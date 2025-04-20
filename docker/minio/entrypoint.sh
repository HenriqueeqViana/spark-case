#!/bin/bash


sleep 10


mc alias set myminio http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD


mc mb myminio/datalake || echo "Bucket 'datalake' já existe."


mc ls myminio
