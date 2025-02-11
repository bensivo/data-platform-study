default:
    just --list


build:
    cd minio && podman build -t minio-init -f minio-init.dockerfile
    cd ingest-api && podman build -t ingest-api -f ingest-api.dockerfile
    cd producer && podman build -t producer -f producer.dockerfile
    cd spark && podman build -t spark-base -f spark-base.dockerfile
    cd spark && podman build -t spark-master -f spark-master.dockerfile
    cd spark && podman build -t spark-worker -f spark-worker.dockerfile
    # cd spark && podman build -t spark-thrift -f spark-thrift.dockerfile


minio:
    podman compose up -d minio minio-init
    open http://localhost:9001

ingest-api:
    podman compose up -d ingest-api

producer:
    podman compose up -d producer

spark:
    # podman compose up -d spark-master spark-worker spark-thrift
    podman compose up -d spark-master spark-worker 
    open http://localhost:8080


etl-bronze:
    podman run \
    --network data-platform \
    -v $PWD/etl:/etl \
    -it \
    spark-base /opt/spark/bin/spark-submit --master spark://spark-master:7077 /etl/bronze.py

etl-query:
    podman run \
    --network data-platform \
    -v $PWD/etl:/etl \
    -it \
    spark-base /opt/spark/bin/spark-submit --master spark://spark-master:7077 /etl/query.py

presto:
    podman compose up -d presto

down: 
    podman compose down