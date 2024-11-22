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
builder.config("spark.sql.catalog.bronze","org.apache.iceberg.spark.SparkCatalog")
builder.config("spark.sql.catalog.bronze.type","hadoop") # NOTE: minio / s3 is a hadoop-compatible filesystem according to spark
builder.config("spark.sql.catalog.bronze.warehouse","s3a://bronze/")

# Configurations for Minio (S3)
#
# NOTE: Just like iceberg, to get this to work, we had to add the hadoop-aws and aws jars to the spark dockerfile
builder.config("spark.hadoop.fs.s3a.access.key", "my-access-key")
builder.config("spark.hadoop.fs.s3a.secret.key", "my-secret-key")
builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
builder.config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

spark = builder.getOrCreate()

df = spark.read.option("header", "true") \
        .json("s3a://raw/foobar/1.0.0/2024/11/22/*.json")

df.printSchema()
df.show()


# "bronze" is the name of our catalog, as configured at the top of the file
# "example" is the name of the database / schema, used for domain separation
# "foobar" is the name of the iceberg table itself
#
table_name = "bronze.example.foobar"

table_exists = spark.catalog.tableExists(table_name)
if not table_exists:
    # If this is our first run ever, the table won't exist, and we need to create it
    df.write.format('iceberg').mode('overwrite').saveAsTable(table_name)
else:
    # On further runs, we don't want to overwrite the whole table, just add to it
    # because this job is meant to be run incrementally.
    df.write.format('iceberg').mode('append').saveAsTable(table_name)

spark.stop()

