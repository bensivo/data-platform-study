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

6. Start the Nessie Catalogs
    ```
    just nessie
    ```

    Nessie is an open-source data catalog maintained by Dremio. Open Source Catalogs is still a rapidly-evolving space, with many in active development right now. As such,
    many integrations are not fully developed.

    Nessie specifically is not a multi-catalog service. If you have 3 catalogs (bronze, silver, gold), you have to run 3 nessie servers. Some other tools, like unitycatalog, support multiple
    catalogs in one server, but they don't have full integration with all the open table formats we want to use.

    The Nessie servers will be at:
        - http://localhost:10001
        - http://localhost:10002
        - http://localhost:10003

    TODO: look into Lakekeeper, a newer Iceberg REST catalog, or Unitycatalog again.

7. Start the Spark + Jupyter server, which will run our ETL scripts
    ```
    just jupyter
    ```

8. Submit a spark job, raw -> bronze
    ```
    just etl-bronze
    ```

    This job reads files from the "RAW" folder, parses them, then writes them to a table in "BRONZE" in iceberg format.


    You can query the data using spark with the below command:
    ```
    just etl-query
    ```

    Or, for a slightly better UX, you can use a query engine, in the next step.

8. Start the Presto Server, our query engine
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

9. Run the bronze -> silver ETL job. Then see the result in presto
   ```
   just etl-silver

   SELECT * from silver.data_platform_example.page_load
   ```

10. Run the silver -> gold ETL job. Then see the result in presto
    ```
    just etl-gold

    SELECT * from gold.data_platform_example.page_loads_per_day where page = '/home' order by date asc 
    ```

11. Run superset
    ```
    just superset
    ```

    Then open your browser to http://localhost:8088

    a.) You'll want to start by deleting all the example datasets.

    b.) Then, go to "Settings" -> "Database Connections" and add 3 databases. Although realistically, you'll only be using the gold database.

    - bronze - presto://presto:8888/bronze
    - silver - presto://presto:8888/silver
    - gold - presto://presto:8888/gold

    c.) Then, go to "Datasets" and import your table "gold.data_platform_example.page_loads_per_day" as a dataset

    d.) Go to "Charts", select your dataset, and create a linegraph
    - x-axis: date
    - metrics: sum(page_load_count)
    - dimensions: page

    e.) Lastly, go to "Dashboards" and create a dashboard. Click and drag your chart onto it


12. Bonus, duckdb
    ```
    just duckdb
    ```

    Duckdb is a great embedded OLAP database that is becoming increasingly popular in world of small-scale analytics and ETL.

    In this project, we show how you can query your nessie + Iceberg + minio tables using Duckdb, and it's much faster than both
    presto and spark. However, it is only really viable for smaller datasets, because duckdb itself can only run on one machine.

    Currently duckdb doesn't support writing tables in iceberg format, but once they do I'll try to update this repo to use Duckdb as
    an alternative to Spark itself for ETL.