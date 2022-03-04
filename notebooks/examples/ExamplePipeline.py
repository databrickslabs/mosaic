# Databricks notebook source
# MAGIC %run "./../setup/EnableMosaic"

# COMMAND ----------

# MAGIC %md
# MAGIC # Example of geospatial pipeline with Mosaic

# COMMAND ----------

environment = 'erni' # This could be: prod / dev / test / etc.

# NOTE: It is recommended to use an external bucket instead of DBFS in your production pipelines

raw_path = f'dbfs:/tmp/mosaic_exmaple/{environment}/raw'
bronze_path = f'dbfs:/tmp/mosaic_exmaple/{environment}/bronze'
silver_path = f'dbfs:/tmp/mosaic_exmaple/{environment}/silver'
# It is recommended to store the gold in a separate bucket from the rest, in order to
# prevent production jobs on bronze/silver to slow down end-user queries on gold tables
gold_path = f'dbfs:/tmp/mosaic_exmaple/{environment}/gold' 

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Land raw data
# MAGIC 
# MAGIC Get the raw data landed on the data lake without any modification. 

# COMMAND ----------

import requests
import os
import pathlib

yellow_taxi_trips_urls = [ 
  'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv',
  'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-02.csv',
#   'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-03.csv',
#   'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-04.csv',
#   'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-05.csv',
#   'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-06.csv',
]

taxi_zones_urls = [ 
  'https://data.cityofnewyork.us/api/geospatial/d3c5-ddgc?method=export&format=GeoJSON' 
]

# The DBFS file system is mounted under /dbfs/ directory on Databricks cluster nodes
raw_local_path = pathlib.Path(raw_path.replace('dbfs:/', '/dbfs/')) 

raw_yellow_taxi_trip_path = raw_local_path / 'yellow_taxi_trip'
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

download(yellow_taxi_trips_urls, raw_yellow_taxi_trip_path, 'csv')

# COMMAND ----------

dbutils.fs.ls(raw_nyc_yellow_taxi_trip.as_uri())

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

def raw_to_bronze_taxi_trip(input_path, output_path):
  
  checkpoint_path = f"{output_path}/_checkpoints"
  schema_path = f"{output_path}/_schema"

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

  # Start the stream with trigger once.
  (df.writeStream.format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(once=True)
    .start(output_path)
  )
  
def raw_to_bronze_zones(input_path, output_path):
  
  checkpoint_path = f"{output_path}/_checkpoints"
  schema_path = f"{output_path}/_schema"

  # Read data as stream with auto loader
  df = (spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        # By specifying an empty schema location, auto loader will 
        # automatically infer the schema from the input files
        .option("cloudFiles.schemaLocation", schema_path)
        .option("multiline", "true") # Geojson has a multiline JSON format
        .load(input_path)
       )

  # Start the stream with trigger once.
  (df.writeStream.format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(once=True)
    .start(output_path)
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tellow taxi trip

# COMMAND ----------

raw_to_bronze_taxi_trip(f"{raw_path}/yellow_taxi_trip", f"{bronze_path}/yellow_taxi_trip")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Taxi zones

# COMMAND ----------

raw_to_bronze_zones(f"{raw_path}/taxi_zones", f"{bronze_path}/taxi_zones")

# COMMAND ----------

import pyspark.sql.functions as f

display(df.select(
 "features",
 "type",
 f.explode(f.col("features.properties")).alias("properties")
).select("*",f.explode(f.col("features.geometry")).alias("geometry")).drop("features")
       )
