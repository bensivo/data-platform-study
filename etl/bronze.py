# bronze.py 
# Reads files from the datalake and writes them to the bronze layer using apache spark


from pyspark.sql import SparkSession

# Create a spark session
builder = SparkSession.builder.appName("bronze")

# Configurations for our apache iceberg catalogs.
# For this job, we're only using the 'bronze' catalog
#
# NOTE: To get iceberg to work, we had to make sure to add the iceberg jars to the spark dockerfile
builder.config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

# Configure our bronze catalog, using hadoop, iceberg, and s3a (minio)
# 
# builder.config("spark.sql.catalog.bronze","org.apache.iceberg.spark.SparkCatalog")
# builder.config("spark.sql.catalog.bronze.type","hadoop") # NOTE: The 'hadoop' catalog option uses object-storage itself as the catalog
# builder.config("spark.sql.catalog.bronze.warehouse", "s3a://bronze/")

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

spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS bronze.data_platform_example
    LOCATION 's3a://bronze/data_platform_example'
""")

df = spark.read \
        .option("recursiveFileLookup", "true") \
        .json("s3a://raw/page_load/v1/")

df.printSchema()
df.show()


# "bronze" is the name of our catalog, as configured at the top of the file
# "example" is the name of the database / schema, used for domain separation
# "foobar" is the name of the iceberg table itself
#
table_name = "bronze.data_platform_example.page_load_v1"

table_exists = spark.catalog.tableExists(table_name)
if not table_exists:
    # If this is our first run ever, the table won't exist, and we need to create it
    df.write.format('iceberg').saveAsTable(table_name)
else:
    # On further runs, we don't want to overwrite the whole table, just add to it
    # because this job is meant to be run incrementally.
    df.write.format('iceberg').mode('append').saveAsTable(table_name)

spark.stop()
