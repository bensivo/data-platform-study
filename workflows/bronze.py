# bronze.py 
# Reads files from the datalake and writes them to the bronze layer using apache spark


from pyspark.sql import SparkSession

# Create a spark session
spark = SparkSession.builder.appName("bronze").getOrCreate()

# context = spark.sparkContext
# context._jsc.hadoopConfiguration().set("fs.s3a.access.key", "my-access-key")
# context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "my-secret-key")
# context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://localhost:9000")
# context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
# context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
# context._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# 
# df = spark.read.option("header", "true") \
#         .json("s3a://raw/foobar/1.0.0/2024/11/20/2024-11-20T06:31:08-7f5c25c221874403a2fbbdd4b3b309cf.json")

columns = ["Name", "Age"]
data = [("Alice", 29), ("Bob", 35), ("Cathy", 23)]
df = spark.createDataFrame(data, columns)
df.show()


spark.stop()
