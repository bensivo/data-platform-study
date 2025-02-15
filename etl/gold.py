# silver.py 
# Reads files from the datalake and writes them to the silver layer using apache spark


from pyspark.sql import SparkSession

# Create a spark session
builder = SparkSession.builder.appName("gold")

# Configure our silver catalog, using nessie, iceberg,  and s3a (minio)
# 
builder.config("spark.sql.catalog.silver","org.apache.iceberg.spark.SparkCatalog")
builder.config("spark.sql.catalog.silver.type", "nessie")
builder.config("spark.sql.catalog.silver.uri", "http://nessie-silver:19120/api/v1")
builder.config("spark.sql.catalog.silver.ref", "main")
builder.config("spark.sql.catalog.silver.authentication.type", "NONE") # BEARER, OAUTH2, AWS
builder.config("spark.sql.catalog.silver.warehouse", "s3a://silver/")

# Configure our gold catalog, using nessie, iceberg,  and s3a (minio)
# 
builder.config("spark.sql.catalog.gold","org.apache.iceberg.spark.SparkCatalog")
builder.config("spark.sql.catalog.gold.type", "nessie")
builder.config("spark.sql.catalog.gold.uri", "http://nessie-gold:19120/api/v1")
builder.config("spark.sql.catalog.gold.ref", "main")
builder.config("spark.sql.catalog.gold.authentication.type", "NONE") # BEARER, OAUTH2, AWS
builder.config("spark.sql.catalog.gold.warehouse", "s3a://gold/")

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
    CREATE SCHEMA IF NOT EXISTS gold.data_platform_example
    LOCATION 's3a://gold/data_platform_example'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS gold.data_platform_example.page_loads_per_day (
    date TIMESTAMP,
    page STRING,
    page_load_count INT
)
USING ICEBERG
LOCATION 's3a://gold/data_platform_example/page_loads_per_day'
""")

spark.sql(f"""
INSERT INTO gold.data_platform_example.page_loads_per_day
SELECT
  date,
  page,
  count(*) as page_count
FROM (
  SELECT
    DATE_TRUNC('DAY', event_ts) as date,
    page
  FROM silver.data_platform_example.page_load
) subquery
GROUP BY date, page
""")
