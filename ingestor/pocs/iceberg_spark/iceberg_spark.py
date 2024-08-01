import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from ckanapi import RemoteCKAN
import os

dadosBh = RemoteCKAN("https://dados.pbh.gov.br/")
packages = dadosBh.action.package_search(rows=10000)["results"]
# Remove empty arrays to avoid erros on schema
packages = map(lambda dict: {k: v for k, v in dict.items() if v != []}, packages)

sparkUrl = "spark://localhost:7077"

os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "password"
os.environ["AWS_REGION"] = "us-east-1"

# sparkUrl = "local"
builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .master(sparkUrl) \
    .config("spark.submit.deployMode","cluster") \
    .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.iceberg:iceberg-aws-bundle:1.4.2') \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "rest") \
    .config("spark.sql.catalog.demo.uri", "http://polaris:8181") \
    .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/wh") \
    .config("spark.sql.catalog.sandbox.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.defaultCatalog", "demo") \
    .config("spark.sql.catalogImplementation", "in-memory")
spark = builder.getOrCreate()

newData = spark.createDataFrame(packages).alias("newData")

tableName = "dataseta"
tablePath = f"s3a://delta-lake/{tableName}"
newData.writeTo("warehouse.datasets").createOrReplace()

# if not DeltaTable.isDeltaTable(spark, tablePath):
#     print("Delta table not found")
#     DeltaTable.createOrReplace(spark)
#     
#     spark.sql(f"CREATE TABLE if not exists {tableName} USING DELTA LOCATION '{tablePath}'")
#     spark.sql(f"ALTER TABLE {tableName} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
#     newData.write.format("delta").save(tablePath)
# else:
#     print("Delta table found, merging")
#     oldData = DeltaTable.forPath(spark, tablePath).alias("oldData")
#     oldData.merge(newData, "oldData.id = newData.id") \
#         .whenMatchedUpdateAll("newData.metadata_modified > oldData.metadata_modified") \
#         .whenNotMatchedInsertAll() \
#         .execute()
