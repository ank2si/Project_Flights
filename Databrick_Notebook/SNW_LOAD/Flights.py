# Databricks notebook source
from  pyspark.sql.functions import input_file_name

# COMMAND ----------

# snowflake connection options
options = {
  "sfUrl": "kq33130.west-us-2.azure.snowflakecomputing.com",
  "sfUser":  "ankit",
  "sfPassword":  "King.15singh90",
  "sfDatabase": "DEMO_DB",
  "sfWarehouse": "COMPUTE_WH"}

# COMMAND ----------

dbutils.widgets.text("FileName","" )

# COMMAND ----------

#loading file from dbfc and defining File location and type
FileName= dbutils.widgets.get("FileName")
file_location = "/FileStore/shared_uploads/ankitsoct1990@gmail.com/"+ FileName+"*.csv"
delimiter = ","


df = spark.read.format("csv") \
     .option("inferSchema", "false") \
     .option("header", "true") \
     .option("sep", delimiter) \
     .load(file_location)
     

# COMMAND ----------

df.write \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "flights") \
  .mode("append")\
  .save()
