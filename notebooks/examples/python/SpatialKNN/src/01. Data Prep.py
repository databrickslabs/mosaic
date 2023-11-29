# Databricks notebook source
# MAGIC %md ## Setup
# MAGIC
# MAGIC > Generates the following in database `mosaic_spatial_knn`: (1) table `building_50k`, (2) table `trip_1m`. These are sufficient samples of the full data for this example. __Note:__ You will need to run the actual Spatial KNN on [Databricks ML Runtime](https://docs.databricks.com/en/release-notes/runtime/index.html), for this one it doesn't matter.
# MAGIC
# MAGIC <p/>
# MAGIC
# MAGIC 1. To use Databricks Labs [Mosaic](https://databrickslabs.github.io/mosaic/index.html) library for geospatial data engineering, analysis, and visualization functionality:
# MAGIC   * Install with `%pip install databricks-mosaic`
# MAGIC   * Import and use with the following:
# MAGIC   ```
# MAGIC   import mosaic as mos
# MAGIC   mos.enable_mosaic(spark, dbutils)
# MAGIC   ```
# MAGIC <p/>
# MAGIC
# MAGIC 2. To use [KeplerGl](https://kepler.gl/) OSS library for map layer rendering:
# MAGIC   * Already installed with Mosaic, use `%%mosaic_kepler` magic [[Mosaic Docs](https://databrickslabs.github.io/mosaic/usage/kepler.html)]
# MAGIC   * Import with `from keplergl import KeplerGl` to use directly
# MAGIC
# MAGIC If you have trouble with Volume access:
# MAGIC
# MAGIC * For Mosaic 0.3 series (< DBR 13)     - you can copy resources to DBFS as a workaround
# MAGIC * For Mosaic 0.4 series (DBR 13.3 LTS) - you will need to either copy resources to DBFS or setup for Unity Catalog + Shared Access which will involve your workspace admin. Instructions, as updated, will be [here](https://databrickslabs.github.io/mosaic/usage/install-gdal.html).
# MAGIC
# MAGIC ---
# MAGIC __Last Updated:__ 27 NOV 2023 [Mosaic 0.3.12]

# COMMAND ----------

# MAGIC %pip install "databricks-mosaic<0.4,>=0.3" --quiet # <- Mosaic 0.3 series
# MAGIC # %pip install "databricks-mosaic<0.5,>=0.4" --quiet # <- Mosaic 0.4 series (as available)

# COMMAND ----------

# -- configure AQE for more compute heavy operations
#  - choose option-1 or option-2 below, essential for REPARTITION!
# spark.conf.set("spark.databricks.optimizer.adaptive.enabled", False) # <- option-1: turn off completely for full control
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False) # <- option-2: just tweak partition management
spark.conf.set("spark.sql.shuffle.partitions", 1_024)                  # <-- default is 200

# -- import databricks + spark functions
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import *

# -- setup mosaic
import mosaic as mos

mos.enable_mosaic(spark, dbutils)
# mos.enable_gdal(spark) # <- not needed for this example

# --other imports
import os
import warnings

warnings.simplefilter("ignore")

# COMMAND ----------

# MAGIC %md __Setup Data Location__
# MAGIC
# MAGIC > You can alter this, of course, to match your preferred location. __Note:__ this is showing DBFS for continuity outside Unity Catalog + Shared Access clusters, but you can easily modify paths to use [Volumes](https://docs.databricks.com/en/sql/language-manual/sql-ref-volumes.html), see more details [here](https://databrickslabs.github.io/mosaic/usage/installation.html) as available.

# COMMAND ----------

user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

raw_path = f"dbfs:/{user_name}/geospatial/mosaic/data/spatial_knn"
raw_fuse_path = raw_path.replace("dbfs:","/dbfs")
dbutils.fs.mkdirs(raw_path)

os.environ['RAW_PATH'] = raw_path
os.environ['RAW_FUSE_PATH'] = raw_fuse_path

print(f"The raw data will be stored in '{raw_path}'")

# COMMAND ----------

building_filename = "nyc_building_footprints.geojson"
os.environ['BUILDING_FILENAME'] = building_filename

# COMMAND ----------

# MAGIC %md __Setup Catalog and Schema__
# MAGIC
# MAGIC > You will have to adjust for your environment.

# COMMAND ----------

catalog_name = "mjohns"
sql(f"USE CATALOG {catalog_name}")

db_name = "mosaic_spatial_knn"
sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
sql(f"USE SCHEMA {db_name}")

# COMMAND ----------

# MAGIC %sql show tables

# COMMAND ----------

# MAGIC %md ## Setup NYC Building Data (`Building` Table | 50K)
# MAGIC
# MAGIC > While the overall data size is ~1.1M, we are going to just take 50K for purposes of this example.

# COMMAND ----------

# MAGIC %md __Download Data (789MB)__

# COMMAND ----------

import requests
import pathlib

def download_url(data_location, dataset_subpath, url):
  fuse_dir = pathlib.Path(data_location.replace('dbfs:',''))
  if (
    not fuse_dir.name.startswith('/Volumes/') and 
    not fuse_dir.name.startswith('/Workspace/')
  ):
    fuse_dir = pathlib.Path(data_location.replace('dbfs:/', '/dbfs/'))
  fuse_dir.mkdir(parents=True, exist_ok=True)
  fuse_path = fuse_dir / dataset_subpath
  if not fuse_path.exists():
    req = requests.get(url)
    with open(fuse_path, 'wb') as f:
      f.write(req.content)
  else:
    print(f"'{fuse_path}' exists...skipping")

# COMMAND ----------

# buildings - data preview = https://data.cityofnewyork.us/Housing-Development/Building-Footprints/nqwf-w8eh
download_url(raw_path, building_filename, "https://data.cityofnewyork.us/api/geospatial/nqwf-w8eh?method=export&format=GeoJSON")

# COMMAND ----------

ls -lh $RAW_FUSE_PATH/$BUILDING_FILENAME

# COMMAND ----------

# MAGIC %md __Generate DataFrame__

# COMMAND ----------

@udf(returnType=StringType())
def fix_geojson(gj_dict):
  """
  This GeoJSON has coordinates nested as a string, 
  so standardize here to avoid issues, gets to same as
  expected when `to_json("feature.geometry")` is
  normally called.
  """
  import json
  
  r_list = []
  for l in gj_dict['coordinates']:
    if isinstance(l,str):
      r_list.append(json.loads(l))
    else:
      r_list.append(l)
  
  return json.dumps(
    {
      "type": gj_dict['type'],
      "coordinates": r_list
    }
  )

# COMMAND ----------

spark.catalog.clearCache() # <- cache useful for dev (avoid recomputes)

_df_geojson_raw = (
  spark.read
    .option("multiline", "true")
    .format("json")
    .load(f"{raw_path}/{building_filename}")
      .select("type", F.explode(col("features")).alias("feature"))
      .repartition(24)
        .select(
          "type", 
          "feature.properties", 
          fix_geojson("feature.geometry").alias("json_geometry")
        )
    .cache()
)

print(f"count? {_df_geojson_raw.count():,}")
display(_df_geojson_raw.limit(1))

# COMMAND ----------

_df_geojson = (
  _df_geojson_raw
    .withColumn("geom", mos.st_geomfromgeojson("json_geometry"))
    .withColumn("geom_wkt", mos.st_astext("geom"))
    .withColumn("is_valid", mos.st_isvalid("geom_wkt"))
    .select("properties.*", "geom_wkt", "is_valid")
)

# print(f"count? {_df_geojson.count():,}")
# display(_df_geojson.limit(1))

# COMMAND ----------

# MAGIC %md __Get Sample of 50K__

# COMMAND ----------

_df_geojson_50k = (
  _df_geojson
    .sample(0.05)
    .limit(50_000)
)

print(f"count? {_df_geojson_50k.count():,}")

# COMMAND ----------

# MAGIC %md __Write out to Delta Lake__

# COMMAND ----------

(
  _df_geojson_50k
    .write
      .format("delta")
      .mode("overwrite")
      .saveAsTable(f"building_50k")
)

# COMMAND ----------

# MAGIC %sql select format_number(count(1), 0) as count from building_50k

# COMMAND ----------

# MAGIC %sql select * from building_50k limit 5

# COMMAND ----------

# MAGIC %md ## Setup NYC Taxi Data (`taxi_trip` | 1M)
# MAGIC
# MAGIC > This data is available as part of `databricks-datasets` for customer. We are just going to take 1M trips for our purposes.
# MAGIC
# MAGIC __Will write sample out to Delta Lake__

# COMMAND ----------

(
  spark.table("delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow`")
    .sample(0.001)
  .withColumn(
    "pickup_point", mos.st_aswkt(mos.st_point(F.col("pickup_longitude"), F.col("pickup_latitude")))
  )
  .withColumn(
    "dropoff_point", mos.st_aswkt(mos.st_point(F.col("dropoff_longitude"), F.col("dropoff_latitude")))
  )
  .limit(1_000_000)
  .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"taxi_trip_1m")
)

# COMMAND ----------

# MAGIC %sql select format_number(count(1), 0) as count from taxi_trip_1m

# COMMAND ----------

# MAGIC %sql select * from taxi_trip_1m limit 5

# COMMAND ----------

# MAGIC %md ## Verify

# COMMAND ----------

# MAGIC %sql show tables from mosaic_spatial_knn

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- notice this is a managed table (see 'Location' col_name)
# MAGIC describe table extended building_50k

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- notice this is a managed table (see 'Location' col_name)
# MAGIC describe table extended taxi_trip_1m

# COMMAND ----------

# MAGIC %md ## Optional: Clean up initial GeoJSON
# MAGIC
# MAGIC > Now that the building data (sample) is in Delta Lake, we don't need it.

# COMMAND ----------

display(dbutils.fs.ls(raw_path))

# COMMAND ----------

# -- uncomment to remove geojson file --
# dbutils.fs.rm(f"{raw_path}/{building_filename}")
