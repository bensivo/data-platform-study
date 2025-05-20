# Utility functions for configuring spark sessions
# 
# Use with the SparkSession.builder object to easily apply configs
# for the given resources
# 
# Example:
# 
#     builder = SparkSession.builder
#     builder.appName("bronze")
#     builder.master("spark://spark-master:7077")
# 
#     spark_config_bronze(builder)
#     spark_config_minio(builder)
# 
#     spark = builder.getOrCreate()

def configure_spark_session(builder):
    # Nessie + Iceberg configs
    builder.config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    builder.config("spark.sql.catalog.example","org.apache.iceberg.spark.SparkCatalog")
    builder.config("spark.sql.catalog.example.type", "nessie")
    builder.config("spark.sql.catalog.example.uri", "http://nessie:19120/api/v1")
    builder.config("spark.sql.catalog.example.ref", "main")
    builder.config("spark.sql.catalog.example.authentication.type", "NONE") # BEARER, OAUTH2, AWS
    builder.config("spark.sql.catalog.example.warehouse", "s3a://")

    # Minio configs
    builder.config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    builder.config("spark.hadoop.fs.s3a.access.key", "my-access-key")
    builder.config("spark.hadoop.fs.s3a.secret.key", "my-secret-key")
    builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
    builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")


# def spark_config_bronze(builder):
#     builder.config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
#     builder.config("spark.sql.catalog.bronze","org.apache.iceberg.spark.SparkCatalog")
#     builder.config("spark.sql.catalog.bronze.type", "nessie")
#     builder.config("spark.sql.catalog.bronze.uri", "http://nessie:19120/api/v1")
#     builder.config("spark.sql.catalog.bronze.ref", "main")
#     builder.config("spark.sql.catalog.bronze.authentication.type", "NONE") # BEARER, OAUTH2, AWS
#     builder.config("spark.sql.catalog.bronze.warehouse", "s3a://bronze/")

# def spark_config_silver(builder):
#     builder.config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
#     builder.config("spark.sql.catalog.silver","org.apache.iceberg.spark.SparkCatalog")
#     builder.config("spark.sql.catalog.silver.type", "nessie")
#     builder.config("spark.sql.catalog.silver.uri", "http://nessie:19120/api/v1")
#     builder.config("spark.sql.catalog.silver.ref", "main")
#     builder.config("spark.sql.catalog.silver.authentication.type", "NONE") # BEARER, OAUTH2, AWS
#     builder.config("spark.sql.catalog.silver.warehouse", "s3a://silver/")

# def spark_config_gold(builder):
#     builder.config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
#     builder.config("spark.sql.catalog.gold","org.apache.iceberg.spark.SparkCatalog")
#     builder.config("spark.sql.catalog.gold.type", "nessie")
#     builder.config("spark.sql.catalog.gold.uri", "http://nessie:19120/api/v1")
#     builder.config("spark.sql.catalog.gold.ref", "main")
#     builder.config("spark.sql.catalog.gold.authentication.type", "NONE") # BEARER, OAUTH2, AWS
#     builder.config("spark.sql.catalog.gold.warehouse", "s3a://gold/")

# def spark_config_minio(builder):
#     builder.config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#     builder.config("spark.hadoop.fs.s3a.access.key", "my-access-key")
#     builder.config("spark.hadoop.fs.s3a.secret.key", "my-secret-key")
#     builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
#     builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
#     builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")