# Databricks notebook source
options = {
  "sfUrl": "kq33130.west-us-2.azure.snowflakecomputing.com",
  "sfUser":  "ankit",
  "sfPassword":  "King.15singh90",
  "sfDatabase": "DEMO_DB",
  "sfWarehouse": "COMPUTE_WH"}

# COMMAND ----------

df = spark.read.format("snowflake")\
    .options(**options)\
    .option("dbtable", "FlIGHTS")\
    .load()

# COMMAND ----------

# MAGIC %sql
# MAGIC df= select month, Airline, count(*) as total_flight
# MAGIC from flights
# MAGIC group by airline, month
# MAGIC order by month asc;
# MAGIC select month, a.airport, count(*) as total_flight
# MAGIC from flights join airports as a on flights.origin_airport= a.IATA_CODE
# MAGIC group by flights.origin_airport,a.airport,month
# MAGIC order by month asc;

# COMMAND ----------

create or replace view Total_Flights as(
(select month, Airline, count(*) as total_flight
from flights
group by airline, month
order by month asc)
union all 
(select month as Month_airport, a.airport, count(*) as total_flight
from flights join airports as a on flights.origin_airport= a.IATA_CODE
group by flights.origin_airport,a.airport,month
order by month asc))

select * from Total_Flights limit 10
