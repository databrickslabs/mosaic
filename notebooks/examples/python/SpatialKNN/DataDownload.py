# Databricks notebook source
# MAGIC %md
# MAGIC ## Install mosaic and enable it

# COMMAND ----------

# MAGIC %pip install databricks-mosaic --quiet

# COMMAND ----------

from pyspark.sql import functions as F
import mosaic as mos

spark.conf.set("spark.databricks.optimizer.adaptive.enabled", "false")
spark.conf.set("spark.databricks.labs.mosaic.geometry.api", "JTS")
from mosaic import enable_mosaic
enable_mosaic(spark, dbutils)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup temporary data location
# MAGIC We will setup a temporary location to store our London Cycling data. </br>

# COMMAND ----------

user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

raw_path = f"dbfs:/tmp/mosaic/{user_name}/data/sdsc"
dbutils.fs.mkdirs(raw_path)

print(f"The raw data will be stored in {raw_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download NYC gis data

# COMMAND ----------

import requests
import pathlib

def download_url(data_location, dataset_subpath, url):
  local_path = pathlib.Path(data_location.replace('dbfs:/', '/dbfs/'))
  local_path.mkdir(parents=True, exist_ok=True)
  req = requests.get(url)
  with open(local_path / dataset_subpath, 'wb') as f:
    f.write(req.content)

# COMMAND ----------

# data preview = https://data.cityofnewyork.us/Transportation/Subway-Entrances/drex-xx56
download_url(raw_path, "nyc_subway_stations.geojson", "https://data.cityofnewyork.us/api/geospatial/drex-xx56?method=export&format=GeoJSON")
# data preview = https://data.cityofnewyork.us/Housing-Development/Building-Footprints/nqwf-w8eh
download_url(raw_path, "nyc_building_footprints.geojson", "https://data.cityofnewyork.us/api/geospatial/nqwf-w8eh?method=export&format=GeoJSON")
# data preview = https://data.cityofnewyork.us/Education/School-Districts/r8nu-ymqj
download_url(raw_path, "nyc_school_districts.geojson", "https://data.cityofnewyork.us/api/geospatial/r8nu-ymqj?method=export&format=GeoJSON")
# data preview = https://data.cityofnewyork.us/Housing-Development/Projects-in-Construction-Map/dzgh-ja44
download_url(raw_path, "nyc_streets.geojson", "https://github.com/fillerwriter/nyc-streets/raw/master/nyc-streets.geojson")

# COMMAND ----------

display(dbutils.fs.ls(raw_path))

# COMMAND ----------

# MAGIC %md
# MAGIC Since these are relatively small files we can safely rely on using geopandas reader for geojson.

# COMMAND ----------

import geopandas as gpd

nyc_building = gpd.read_file("/dbfs/tmp/mosaic/milos.colic@databricks.com/data/sdsc/nyc_building_footprints.geojson")
nyc_subway_stations = gpd.read_file("/dbfs/tmp/mosaic/milos.colic@databricks.com/data/sdsc/nyc_subway_stations.geojson")
nyc_streets = gpd.read_file("/dbfs/tmp/mosaic/milos.colic@databricks.com/data/sdsc/nyc_streets.geojson")

# COMMAND ----------

nyc_building["geometry"] = nyc_building["geometry"].apply(lambda x: x.wkt)
nyc_subway_stations["geometry"] = nyc_subway_stations["geometry"].apply(lambda x: x.wkt)
nyc_streets["geometry"] = nyc_streets["geometry"].apply(lambda x: x.wkt)

# COMMAND ----------

nyc_building_sdf = spark.createDataFrame(nyc_building) 
nyc_subway_stations_sdf = spark.createDataFrame(nyc_subway_stations) 
nyc_streets_sdf = spark.createDataFrame(nyc_streets) 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS sdsc_database

# COMMAND ----------

nyc_building_sdf.repartition(20).write.format("delta").saveAsTable("sdsc_database.buildings")
nyc_subway_stations_sdf.repartition(20).write.format("delta").saveAsTable("sdsc_database.subway_stations")
nyc_streets_sdf.repartition(20).write.format("delta").saveAsTable("sdsc_database.streets")

# COMMAND ----------

# MAGIC %md
# MAGIC Note that we are taking > 1M of NYC taxi data with our sampling.

# COMMAND ----------

taxi_trips = spark.table("delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow`").sample(0.001)
taxi_trips = taxi_trips.withColumn(
  "pickup_point", mos.st_aswkt(mos.st_point(F.col("pickup_longitude"), F.col("pickup_latitude")))
).withColumn(
  "dropoff_point", mos.st_aswkt(mos.st_point(F.col("dropoff_longitude"), F.col("dropoff_latitude")))
)
taxi_trips.write.format("delta").mode("overwrite").saveAsTable("sdsc_database.taxi_trips")

# COMMAND ----------


