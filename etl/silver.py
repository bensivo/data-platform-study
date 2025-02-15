# silver.py 
# Reads files from the datalake and writes them to the silver layer using apache spark


from pyspark.sql import SparkSession

# Create a spark session
builder = SparkSession.builder.appName("silver")

# Configurations for our apache iceberg catalogs.
# For this job, we're only using the 'silver' catalog
#
# NOTE: To get iceberg to work, we had to make sure to add the iceberg jars to the spark dockerfile
builder.config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

# Use a Hadoop catalog, storing metadata in the file system itself
# The easiest solution, but lacks some key feautres (esp in quering)
# 
# builder.config("spark.sql.catalog.bronze","org.apache.iceberg.spark.SparkCatalog")
# builder.config("spark.sql.catalog.bronze.type","hadoop") # NOTE: The 'hadoop' catalog option uses object-storage itself as the catalog
# builder.config("spark.sql.catalog.bronze.warehouse", "s3a://bronze/")

# Configure our bronze catalog, using nessie, iceberg,  and s3a (minio)
# 
builder.config("spark.sql.catalog.bronze","org.apache.iceberg.spark.SparkCatalog")
builder.config("spark.sql.catalog.bronze.type", "nessie")
builder.config("spark.sql.catalog.bronze.uri", "http://nessie-bronze:19120/api/v1")
builder.config("spark.sql.catalog.bronze.ref", "main")
builder.config("spark.sql.catalog.bronze.authentication.type", "NONE") # BEARER, OAUTH2, AWS
builder.config("spark.sql.catalog.bronze.warehouse", "s3a://bronze/")

# Configure our silver catalog, using nessie, iceberg,  and s3a (minio)
# 
builder.config("spark.sql.catalog.silver","org.apache.iceberg.spark.SparkCatalog")
builder.config("spark.sql.catalog.silver.type", "nessie")
builder.config("spark.sql.catalog.silver.uri", "http://nessie-silver:19120/api/v1")
builder.config("spark.sql.catalog.silver.ref", "main")
builder.config("spark.sql.catalog.silver.authentication.type", "NONE") # BEARER, OAUTH2, AWS
builder.config("spark.sql.catalog.silver.warehouse", "s3a://silver/")

# Configure the S3a filesystem
#
builder.config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
builder.config("spark.hadoop.fs.s3a.access.key", "my-access-key")
builder.config("spark.hadoop.fs.s3a.secret.key", "my-secret-key")
builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

spark = builder.getOrCreate()

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS silver.data_platform_example
    LOCATION 's3a://silver/data_platform_example'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.data_platform_example.page_load (
    event_name STRING,
    event_version STRING,
    event_ts TIMESTAMP,
    page STRING,
    user_name STRING,
    browser STRING
)
USING ICEBERG
LOCATION 's3a://silver/data_platform_example/page_load'
""")

df = spark.sql(f"""
SELECT
    metadata.name AS event_name,
    metadata.version AS event_version,
    CAST(metadata.timestamp AS TIMESTAMP) AS event_ts,
    payload.page AS page,
    payload.user_name AS user_name,
    payload.browser AS browser
FROM bronze.data_platform_example.page_load_v1
""")

df = df.dropDuplicates()

df.write \
    .format("iceberg") \
    .mode("overwrite") \
    .saveAsTable("silver.data_platform_example.page_load")
