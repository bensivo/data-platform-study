# Data Platform Study

This repo is a personal study on Data Engineering / Analytics pipelines, and the tools that make them up.

## Walkthrough

1. Build all containers
    ```
    just build
    ```

2. Run Minio
    ```
    just minio
    ```

    Then open minio in your browser at http://localhost:9000. 
    
    The username and pasword are `my-access-key` and `my-secret-key`

3. Run the Ingest API
    ```
    just ingest-api
    ```

    This starts a python API at http://localhost:8000. It exposes a single endpoint at `POST /event`, when you POST data to this endpoint, it'll get written to minio in the RAW bucket, with some metadata (NOTE: it does some batching internally). 

4. Generate some data
    ```
    just producer
    ```

    The producer app will make start sending requests to the ingest API, which will write them to minio.

    Go to your minio browser tab, you should see data in the folder `raw/page_load/v1/<year>/<month>/<date>`

5. Start the Spark Cluster
    ```
    just spark
    ```

    The Spark UI will be available in your browser at http://localhost:8080. You should see the connected worker node, but no running applications.

6. Submit a spark job, raw -> bronze
    ```
    just etl-bronze
    ```

    This job reads files from the "RAW" folder, parses them, then writes them to a table in "BRONZE" in iceberg format

7. Query the data from the bronze table, also using spark
    ```
    just etl-query
    ```

    TODO: figure out how to get spark-thriftserver working so that I can query with a normal db interface.