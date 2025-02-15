import json
import duckdb
import requests

# Configure connection to minio
duckdb.sql("""
    INSTALL httpfs;
    LOAD https;

    CREATE SECRET (
        TYPE S3,
        REGION 'us-east-1',
        URL_STYLE 'path',
        ENDPOINT 'localhost:9000',
        KEY_ID 'my-access-key' ,
        SECRET 'my-secret-key',
        USE_SSL false
    );

    INSTALL iceberg;
    LOAD iceberg;
    UPDATE EXTENSIONS(iceberg);
""")

def get_table( nessie_url: str, schema: str, table: str):
    """
    Query an iceberg table using Nessie and DuckDB

    Params:
        nessie_url: the base url of the nessie instance. e.g. 'http://localhost:19092'
        schema: name of the schema
        table: name of the table
    
    Returns:
        A duckdb relation object, containing the entire table
    """
    # First, query our bronze nessie instance for the metadata location of this table, using their REST API
    res = requests.post(
        url=f"{nessie_url}/api/v1/contents?ref=main", 
        json={
            'requestedKeys': [
                {
                    'elements': [schema, table]
                }
            ]
        }
    )
    metadata = json.loads(res.content)
    metadata_location = metadata['contents'][0]['content']['metadataLocation']


    # Then, use duckdb's "iceberg_scan" function to read the iceberg table that's referenced by that metadata file
    r = duckdb.sql(f"""
    SELECT * from iceberg_scan('{metadata_location}')
    """)
    return r

print('bronze.data_platform_example.page_load_v1')
r = get_table(nessie_url='http://localhost:10001', schema='data_platform_example', table='page_load_v1')
r.show()

print('silver.data_platform_example.page_load')
r = get_table(nessie_url='http://localhost:10002', schema='data_platform_example', table='page_load')
r.show()

print('gold.data_platform_example.page_loads_per_day')
r = get_table(nessie_url='http://localhost:10003', schema='data_platform_example', table='page_loads_per_day')
r.show()

