default:
    just --list


build:
    cd minio && podman build -t minio-init -f minio-init.dockerfile
    cd ingest && podman build -t ingest -f ingest.dockerfile
    cd spark && podman build -t spark-base -f spark-base.dockerfile
    cd spark && podman build -t spark-master -f spark-master.dockerfile
    cd spark && podman build -t spark-worker -f spark-worker.dockerfile

up:
    podman compose up -d

logs: 
    podman compose logs -f

down: 
    podman compose down

push:
    curl \
    --request POST \
    --url http://localhost:8000/event \
    --header 'Content-Type: application/json' \
    --data '{"metadata": {"name": "foobar", "version":"1.0.0", "timestamp":"asdf"}, "payload": {"foo":"bar"}}'

bronze:
    podman run \
    --network data-platform \
    -v $PWD/workflows:/workflows \
    -it \
    spark-base /opt/spark/bin/spark-submit --master spark://spark-master:7077 /workflows/bronze.py
