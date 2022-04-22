# Databricks notebook source
# MAGIC %pip install folium

# COMMAND ----------

from mosaic import enable_mosaic
enable_mosaic(spark, dbutils)

# COMMAND ----------

# MAGIC %md
# MAGIC # Example of geospatial pipeline with Mosaic

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

environment = 'erni' # This could be: prod / dev / test / etc.

# NOTE: It is recommended to use an external bucket instead of DBFS in your production pipelines

# Raw data
raw_path = f'dbfs:/tmp/mosaic_exmaple/{environment}/raw'

# Bronze
bronze_path = f'dbfs:/tmp/mosaic_exmaple/{environment}/bronze'
bronze_db = f'{environment}_bronze'

# Silver
silver_path = f'dbfs:/tmp/mosaic_exmaple/{environment}/silver'
silver_db = f'{environment}_silver'

# Gold
# It is recommended to store the gold in a separate bucket from the rest, in order to
# prevent production jobs on bronze/silver to slow down end-user queries on gold tables
gold_path = f'dbfs:/tmp/mosaic_exmaple/{environment}/gold'
gold_db = f'{environment}_gold'

# Metadata
metadata_path = f'dbfs:/tmp/mosaic_exmaple/{environment}/metadata'

# COMMAND ----------

# dbutils.fs.rm(raw_path, True)
# dbutils.fs.rm(bronze_path, True)
# dbutils.fs.rm(silver_path, True)
# dbutils.fs.rm(gold_path, True)
# dbutils.fs.rm(metadata_path, True)

# COMMAND ----------

# Create databases
spark.sql(f"CREATE DATABASE IF NOT EXISTS {bronze_db} LOCATION '{bronze_path}'")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {silver_db} LOCATION '{silver_path}'")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {gold_db} LOCATION '{gold_path}'")

# COMMAND ----------

# MAGIC 
# MAGIC %md 
# MAGIC ## Land raw data
# MAGIC 
# MAGIC Get the raw data landed on the data lake without any modification. 

# COMMAND ----------


def simulate_random_nyc_taxi_data(n_rows):
  
  ts_start_ms = 1516364153
  ts_span_s = 365 * 24 * 60 * 60
  trip_max_duration_s = 30 * 60
  
  min_lat = 40.513189
  max_lat = 40.884033
  
  min_long = -74.279095
  max_long = -73.672070
  
  return (spark.range(n_rows)
          .withColumn("rand_timestamp", (f.lit(ts_start_ms) + f.rand() * ts_span_s).cast("long"))
          .withColumn("tpep_pickup_datetime", f.col("rand_timestamp").cast("timestamp"))
          .withColumn("tpep_dropoff_datetime", (f.col("rand_timestamp") + f.rand() * trip_max_duration_s).cast("timestamp"))
          
          .withColumn("Dropoff_longitude", f.rand() * (max_long - min_long) + min_long)
          .withColumn("Dropoff_latitude", f.rand() * (max_lat - min_lat) + min_lat)
          .withColumn("Pickup_longitude", f.rand() * (max_long - min_long) + min_long)
          .withColumn("Pickup_latitude", f.rand() * (max_lat - min_lat) + min_lat)
          
          .withColumn("Passenger_count", (f.rand() * 4 + 1).cast("int"))
          
          .withColumn("Fare_amount", (f.rand() * 50 * 100 + 500).cast("int") / 100)
          .withColumn("Tip_amount", (f.rand() * 15 * 100).cast("int") / 100)
          .withColumn("Total_amount", f.col("Tip_amount") + f.col("Tip_amount"))
          
          .drop("rand_timestamp")
         )

# COMMAND ----------

import requests
import os
import pathlib

# yellow_taxi_trips_urls = [ 
#   'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv',
#   'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-02.csv',
# #   'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-03.csv',
# #   'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-04.csv',
# #   'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-05.csv',
# #   'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-06.csv',
# ]

taxi_zones_urls = [ 
  'https://data.cityofnewyork.us/api/geospatial/d3c5-ddgc?method=export&format=GeoJSON' 
]

# The DBFS file system is mounted under /dbfs/ directory on Databricks cluster nodes
raw_local_path = pathlib.Path(raw_path.replace('dbfs:/', '/dbfs/')) 

raw_taxi_zones_path = raw_local_path / 'taxi_zones'

def download(urls, folder, extension='csv'):
  
  dbutils.fs.rm(folder.as_uri(), True)
  folder.mkdir(parents=True, exist_ok=True)
  
  for i, url in enumerate(urls):
    req = requests.get(url)
    with open(folder  / f'{i}.{extension}', 'wb') as f:
      f.write(req.content)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load taxi trips

# COMMAND ----------

(simulate_random_nyc_taxi_data(1000000)
   .write
   .option("header", True)
   .mode("append")
   .csv(f"{raw_path}/yellow_taxi_trip")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load taxi zones

# COMMAND ----------

download(taxi_zones_urls, raw_taxi_zones_path, 'geojson')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze
# MAGIC 
# MAGIC See the documentation of [Autoloader](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Functions

# COMMAND ----------


def write_stream(df, output_table, checkpoint_path):
  # Start the stream with trigger once.
  return (df.writeStream.format("delta")
      .option("checkpointLocation", checkpoint_path)
      .option("mergeSchema", "true")
      .trigger(once=True)
      .toTable(output_table)
    )

def raw_to_bronze_yellow_taxi_trip(input_path, metadata_path, output_table):
  
  checkpoint_path = f"{metadata_path}/_checkpoints/{output_table}"
  schema_path = f"{metadata_path}/_schema/{output_table}"

  # Read data as stream with auto loader
  df = (spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        # By specifying an empty schema location, auto loader will 
        # automatically infer the schema from the input files
        .option("cloudFiles.schemaLocation", schema_path)
        .option("header", "true")
        .load(input_path)
       )

  return write_stream(df, output_table, checkpoint_path)
  
  
def raw_to_bronze_taxi_zones(input_path, metadata_path, output_table):
  
  checkpoint_path = f"{metadata_path}/_checkpoints/{output_table}"
  schema_path = f"{metadata_path}/_schema/{output_table}"

  # Read data as stream with auto loader
  df = (spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", True)
        .option("cloudFiles.schemaLocation", schema_path)
        .option("multiline", "true") # Geojson has a multiline JSON format
        .load(input_path)
       )

  return write_stream(df, output_table, checkpoint_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Yellow taxi trip

# COMMAND ----------

raw_to_bronze_yellow_taxi_trip(
  f"{raw_path}/yellow_taxi_trip/*.csv", 
  metadata_path, 
  f"{bronze_db}.yellow_taxi_trip"
).awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Taxi zones

# COMMAND ----------

raw_to_bronze_taxi_zones(
  f"{raw_path}/taxi_zones",
  metadata_path,
  f"{bronze_db}.taxi_zones"
).awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver
# MAGIC 
# MAGIC Silver tables contain cleaned and formatted data ready to be used.

# COMMAND ----------

spark.table(f"{bronze_db}.yellow_taxi_trip").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Functions

# COMMAND ----------

def clean_yellow_taxi_trip(df):
  return (df

   # Standardize column naming and select columns of interest
   .select(
     f.col("tpep_pickup_datetime").alias("pickup_time"),
     f.col("tpep_dropoff_datetime").alias("dropoff_time"),
     f.col("Pickup_longitude").alias("pickup_longitude"),
     f.col("Pickup_latitude").alias("pickup_latitude"),
     f.col("Dropoff_longitude").alias("dropoff_longitude"),
     f.col("Dropoff_latitude").alias("dropoff_latitude"),

     f.col("Fare_amount").alias("fare_amount_usd"),
     f.col("Tip_amount").alias("tip_amount_usd"),
     f.col("Total_amount").alias("total_amount_usd")
   )
 
   # Standardize data types
   .withColumn("pickup_time", f.col("pickup_time").cast("timestamp"))
   .withColumn("dropoff_time", f.col("dropoff_time").cast("timestamp"))
                       
   # Add data quality metadata
   # Here you cound use more sofisticated checks with a library like https://greatexpectations.io/ or https://github.com/awslabs/deequ
   .withColumn("is_valid", f.col("total_amount_usd").between(0, 1000))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tests

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ### Yellow taxi trip

# COMMAND ----------

(spark
   .table(f"{bronze_db}.yellow_taxi_trip")
   .transform(clean_yellow_taxi_trip)
   .write
   .mode("overwrite")
   .saveAsTable(f"{silver_db}.yellow_taxi_trip_clean")
)

# COMMAND ----------

df = spark.table(f"{bronze_db}.taxi_zones")

# COMMAND ----------



# COMMAND ----------

from mosaic import st_geomfromgeojson, st_aswkb

def clean_taxi_zones(df):
  return (df
          .select("type", f.explode(f.col("features")).alias("feature"))
          .select("type", f.col("feature.properties").alias("properties"), f.to_json(f.col("feature.geometry")).alias("json_geometry"))
          .withColumn("geometry", st_aswkb(st_geomfromgeojson("json_geometry")))
         )

display(clean_taxi_zones(spark.table(f"{bronze_db}.taxi_zones")))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Plot example

# COMMAND ----------

tmp = clean_taxi_zones(spark.table(f"{bronze_db}.taxi_zones")).cache()

# COMMAND ----------

import folium

# Collect the geojson column from the dataset into a list
def plot_geojsons(df, column_name):
  geojsons = df.select(column_name).collect()

  m = folium.Map(location=[0, 0], zoom_start=2)
  for row in geojsons:
    folium.GeoJson(row[column_name], name="geojson").add_to(m)
    
  return m

# COMMAND ----------

plot_geojsons(tmp.limit(10), "json_geometry")

# COMMAND ----------


