# Raspadinha


## Setup instructions 

1. Install `venv`  
   `pip install virtualenv`
2. Create the `venv`  
   `python3 -m venv env`
3. Active the `venv`  
   `source env/bin/activate`
4. Install python dependencies  
   `pip install`

## Run Instructions 

1. Start spark server  
   `docker compose up`
2. Run pyspark 
   `pyspark --packages io.delta:delta-spark_2.12:3.1.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"`