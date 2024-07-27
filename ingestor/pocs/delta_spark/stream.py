import pyspark
from delta import *
from pyspark.sql.functions import *

minioUrl = "http://localhost:9000"
connectionTimeOut = "5000"
# sparkUrl = "spark://localhost:7077"
sparkUrl = "local"
builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .master(sparkUrl) \
    .config("spark.hadoop.fs.s3a.access.key", "access_key") \
    .config("spark.hadoop.fs.s3a.secret.key", "secret_key") \
    .config("spark.hadoop.fs.s3a.endpoint", minioUrl) \
    .config("spark.hadoop.fs.s3a.connection.timeout", connectionTimeOut) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder, 
                                       extra_packages=[
                                           "org.apache.hadoop:hadoop-aws:3.3.4"
                                        ]) \
    .getOrCreate()

tableName = "s3://delta-lake/demo2"
stream2 = spark.readStream.format("delta").option("readChangeFeed", "true").option("startingVersion", 1).load(tableName).writeStream.format("console").start().awaitTermination()