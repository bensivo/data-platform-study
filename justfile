default:
    just --list


build:
    cd minio && podman build -t minio-init -f minio-init.dockerfile
    cd ingest-api && podman build -t ingest-api -f ingest-api.dockerfile
    cd producer && podman build -t producer -f producer.dockerfile
    cd spark && podman build -t spark-base -f spark-base.dockerfile
    cd spark && podman build -t spark-master -f spark-master.dockerfile
    cd spark && podman build -t spark-worker -f spark-worker.dockerfile
    cd etl && podman build -t spark-jupyter -f spark-jupyter.dockerfile
    # cd unitycatalog && podman build -t unitycatalog-ui -f unitycatalog-ui.dockerfile

minio:
    podman compose up -d minio minio-init
    open http://localhost:9001

ingest-api:
    podman compose up -d ingest-api

producer:
    podman compose up -d producer

spark:
    podman compose up -d spark-master spark-worker 
    open http://localhost:8080

nessie:
    podman compose up -d nessie-bronze nessie-silver nessie-gold
    open http://localhost:10001
    open http://localhost:10002
    open http://localhost:10003

jupyter:
    podman compose up -d spark-jupyter
    open http://localhost:8082


presto:
    podman compose up -d presto
    open http://localhost:8888

# unitycatalog:
#     podman compose up -d unitycatalog-ui unitycatalog-server

superset:
    podman compose up -d redis db superset superset-init superset-worker superset-worker-beat

etl-bronze:
    podman run  \
    --network data-platform \
    --name etl-bronze \
    -v $PWD/etl/:/etl \
    -it \
    --rm \
    spark-base /opt/spark/bin/spark-submit --master spark://spark-master:7077 /etl/bronze.py

etl-query:
    podman run \
    --network data-platform \
    --name etl-query \
    -v $PWD/etl/:/etl \
    -it \
    --rm \
    spark-base /opt/spark/bin/spark-submit --master spark://spark-master:7077 /etl/query.py

etl-silver:
    podman run \
    --network data-platform \
    --name etl-silver \
    -v $PWD/etl/:/etl \
    -it \
    --rm \
    spark-base /opt/spark/bin/spark-submit --master spark://spark-master:7077 /etl/silver.py

etl-gold:
    podman run \
    --network data-platform \
    --name etl-gold \
    -v $PWD/etl:/etl \
    -it \
    --rm \
    spark-base /opt/spark/bin/spark-submit --master spark://spark-master:7077 /etl/gold.py

duckdb:
    cd duckdb && uv run python ./main.py

down: 
    podman compose down