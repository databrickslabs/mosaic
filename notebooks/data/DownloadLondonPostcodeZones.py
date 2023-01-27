# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup temporary data location
# MAGIC We will setup a temporary location to store our London Postcode shapes. </br>

# COMMAND ----------

user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

raw_path = f"dbfs:/tmp/mosaic/{user_name}"
raw_postcodes_path = f"{raw_path}/postcodes"

print(f"The raw data will be stored in {raw_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download postcode zones GeoJSON

# COMMAND ----------

import requests
import pathlib

postcodes_url = 'https://raw.githubusercontent.com/sjwhitworth/london_geojson/master/london_postcodes.json'

# The DBFS file system is mounted under /dbfs/ directory on Databricks cluster nodes

local_postcodes_path = pathlib.Path(raw_postcodes_path.replace('dbfs:/', '/dbfs/'))
local_postcodes_path.mkdir(parents=True, exist_ok=True)

req = requests.get(postcodes_url)
with open(local_postcodes_path / f'london_postcodes.geojson', 'wb') as f:
  f.write(req.content)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify GeoJSON was downloaded

# COMMAND ----------

display(dbutils.fs.ls(raw_postcodes_path))
