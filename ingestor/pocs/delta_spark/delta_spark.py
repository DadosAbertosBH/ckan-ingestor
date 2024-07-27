import pyspark
import pyspark.pandas as ps
from delta import *
import dataset_fetcher

minioUrl = "http://localhost:9000"
connectionTimeOut = "5000"
# sparkUrl = "spark://localhost:7077"
sparkUrl = "local"

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .master(sparkUrl) \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")  \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
    .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.endpoint", minioUrl) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.timeout", connectionTimeOut) \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() 

spark = configure_spark_with_delta_pip(builder, 
                                       extra_packages=[
                                           "org.apache.hadoop:hadoop-aws:3.3.4",
                                           "com.google.errorprone:error_prone_annotations:2.28.0",
                                           "io.delta:delta-iceberg_2.12:3.2.0",
                                        ]) \
    .getOrCreate()

tableName = "dataseta"
tablePath = f"s3a://warehouse/{tableName}"

df = dataset_fetcher.fetch("https://dados.pbh.gov.br/")
sparkdf = ps.from_pandas(df.to_pandas()).to_spark()

if not spark.catalog.tableExists(tableName):
    print("Delta table not found")

    spark.sql(f"""CREATE TABLE if not exists {tableName} USING DELTA LOCATION '{tablePath}'
        TBLPROPERTIES (
                    'delta.minReaderVersion' = '2',
                    'delta.minWriterVersion' = '5',
                    'delta.columnMapping.mode' = 'name',
                    'delta.enableChangeDataFeed' = 'true',
                    'delta.enableIcebergCompatV2' = 'true',              
                    'delta.universalFormat.enabledFormats' = 'iceberg'
            )              
    """)
    sparkdf.write.mode("overwrite").save(tableName)    
else:
    print("Delta table found, merging")    
    sparkdf.write.mode("overwrite").save(tableName)    
    # oldData = DeltaTable.forPath(spark, tablePath).alias("oldData")
    # oldData.merge(newData, "oldData.id = newData.id") \
    #     .whenMatchedUpdateAll("newData.metadata_modified > oldData.metadata_modified") \
    #     .whenNotMatchedInsertAll() \
    #     .execute()
