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

    ```
    curl \
    --request POST \
    --url http://localhost:8000/event \
    --header 'Content-Type: application/json' \
    --data '{
        "metadata":{
            "name":"example", 
            "version":"v1", 
            "timestamp":"1970-01-01T00:00:00"
        }, 
        "payload": {
            "foo":"bar"
        }
    }'
    ```

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

    This job reads files from the "RAW" folder, parses them, then writes them to a table in "BRONZE" in iceberg format.


    You can query the data using spark with the below command:
    ```
    just etl-query
    ```

    Or, for a slightly better UX, you can use a query engine, in the next step.

7. Start the Presto Server, our query engine
    ```
    just presto
    ```

    The Preso UI will be at http://localhost:8888. Find the SQL Query editor, and submit this query
    ```
    SELECT * FROM bronze.data_platform_example.page_load_v1 

    SELECT metadata.name, metadata.version, metadata.timestamp, payload.page, payload.user_name, payload.browser from bronze.data_platform_example.page_load_v1 limit 100
    ```

    The Presto SQL UI is meant for exploration, not production use, so it limits results to 100 rows. However, presto has other clients, including a REST API
    ```
    curl \
    --request POST \
    --header "X-Presto-User: admin" \
    --data "SELECT metadata.name, metadata.version, metadata.timestamp, payload.page, payload.user_name, payload.browser from bronze.data_platform_example.page_load_v1" \
    --url http://localhost:8888/v1/statement \
    | jq
    ```

    NOTE: This is an async API, you'll get a JSON response with a "nextUri" field. Make a GET request to that uri to get either a "queued" message or your results. 

8. Run the bronze -> silver ETL job. Then see the result in presto
   ```
   SELECT * from silver.data_platform_example.page_load
   ```

9. Run the silver -> gold ETL job. Then see the result in presto
   ```
   SELECT * from gold.data_platform_example.page_loads_per_day where page = '/home' order by date asc 
   ```