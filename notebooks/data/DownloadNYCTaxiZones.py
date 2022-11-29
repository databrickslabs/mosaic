# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup temporary data location
# MAGIC We will setup a temporary location to store our New York City Neighbourhood shapes. </br>

# COMMAND ----------

user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

raw_path = f"dbfs:/tmp/mosaic/{user_name}"
raw_taxi_zones_path = f"{raw_path}/taxi_zones"

print(f"The raw data will be stored in {raw_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download taxi zones GeoJSON

# COMMAND ----------

import requests
import pathlib

taxi_zones_url = 'https://data.cityofnewyork.us/api/geospatial/d3c5-ddgc?method=export&format=GeoJSON'

# The DBFS file system is mounted under /dbfs/ directory on Databricks cluster nodes


local_taxi_zones_path = pathlib.Path(raw_taxi_zones_path.replace('dbfs:/', '/dbfs/'))
local_taxi_zones_path.mkdir(parents=True, exist_ok=True)

req = requests.get(taxi_zones_url)
with open(local_taxi_zones_path / f'nyc_taxi_zones.geojson', 'wb') as f:
  f.write(req.content)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify GeoJSON was downloaded

# COMMAND ----------

display(dbutils.fs.ls(raw_taxi_zones_path))
