# query.py 
# Run a SQL query against the data in spark


from pyspark.sql import SparkSession

# Create a spark session
builder = SparkSession.builder.appName("query")

# Configurations for our apache iceberg catalogs.
# For this job, we're only using the 'bronze' catalog
#
# NOTE: To get iceberg to work, we had to make sure to add the iceberg jars to the spark dockerfile
builder.config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
builder.config("spark.sql.catalog.bronze","org.apache.iceberg.spark.SparkCatalog")
builder.config("spark.sql.catalog.bronze.type","hadoop") # NOTE: The 'hadoop' catalog option uses object-storage itself as the catalog
builder.config("spark.sql.catalog.bronze.warehouse","s3a://bronze/")

# Configurations for our object-storage service, Minio
#
# NOTE: Just like iceberg, to get this to work, we had to add the hadoop-aws and aws jars to the spark dockerfile
builder.config("spark.hadoop.fs.s3a.access.key", "my-access-key")
builder.config("spark.hadoop.fs.s3a.secret.key", "my-secret-key")
builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
builder.config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

spark = builder.getOrCreate()

df = spark.sql(f"""
    SELECT metadata.name, metadata.version, metadata.timestamp, payload.page, payload.user_name, payload.browser 
    FROM bronze.data_platform_example.page_load_v1 LIMIT 10
""")

df.show()

df = spark.sql(f"""
    SELECT COUNT(*)
    FROM bronze.data_platform_example.page_load_v1
""")
df.show()

spark.stop()