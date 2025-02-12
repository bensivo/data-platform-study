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

# Configurations for our silver catalog, using the hadoop catalog type
#
# NOTE: Just like iceberg, to get this to work, we had to add the hadoop-aws and aws jars to the spark dockerfile
builder.config("spark.sql.catalog.silver","org.apache.iceberg.spark.SparkCatalog")
builder.config("spark.sql.catalog.silver.type","hadoop") # NOTE: The 'hadoop' catalog option uses object-storage itself as the catalog
builder.config("spark.hadoop.fs.s3a.access.key", "my-access-key")
builder.config("spark.hadoop.fs.s3a.secret.key", "my-secret-key")
builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
builder.config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
builder.config("spark.sql.catalog.silver.warehouse", "s3a://silver/")

# Configurations for our gold catalog, using the hadoop catalog type
#
# NOTE: Just like iceberg, to get this to work, we had to add the hadoop-aws and aws jars to the spark dockerfile
builder.config("spark.sql.catalog.gold","org.apache.iceberg.spark.SparkCatalog")
builder.config("spark.sql.catalog.gold.type","hadoop") # NOTE: The 'hadoop' catalog option uses object-storage itself as the catalog
builder.config("spark.hadoop.fs.s3a.access.key", "my-access-key")
builder.config("spark.hadoop.fs.s3a.secret.key", "my-secret-key")
builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
builder.config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
builder.config("spark.sql.catalog.gold.warehouse", "s3a://gold/")


spark = builder.getOrCreate()


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
