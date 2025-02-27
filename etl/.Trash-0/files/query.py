# query.py 
# Run a SQL query against the data in spark


from pyspark.sql import SparkSession

# Create a spark session
builder = SparkSession.builder.appName("query")
builder.config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

# Configure our bronze catalog, using nessie, iceberg, and s3a (minio)
# 
builder.config("spark.sql.catalog.bronze","org.apache.iceberg.spark.SparkCatalog")
builder.config("spark.sql.catalog.bronze.type", "nessie")
builder.config("spark.sql.catalog.bronze.uri", "http://nessie-bronze:19120/api/v1")
builder.config("spark.sql.catalog.bronze.ref", "main")
builder.config("spark.sql.catalog.bronze.authentication.type", "NONE") # BEARER, OAUTH2, AWS
builder.config("spark.sql.catalog.bronze.warehouse", "s3a://bronze/")

# Configure our bronze catalog, using unitycatalog, iceberg, and s3a (minio)
# (NOTE: UnityCatalog is a WIP. I haven't been able to get it to work 100%)
# 
# builder.config("spark.sql.catalog.spark_catalog", "io.unitycatalog.spark.UCSingleCatalog")
# builder.config("spark.sql.catalog.bronze", "io.unitycatalog.spark.UCSingleCatalog")
# builder.config("spark.sql.catalog.bronze.uri", "http://unitycatalog-server:8080")
# builder.config("spark.sql.catalog.bronze.token", "")
# builder.config("spark.sql.catalog.bronze.warehouse", "s3a://bronze/")

# Configure the S3a filesystem
#
builder.config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
builder.config("spark.hadoop.fs.s3a.access.key", "my-access-key")
builder.config("spark.hadoop.fs.s3a.secret.key", "my-secret-key")
builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

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