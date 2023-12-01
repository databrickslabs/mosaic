# Databricks notebook source
# MAGIC %md # Mosaic Quickstart
# MAGIC
# MAGIC > Perform a point-in-polygon spatial join between NYC Taxi trips and zones. __Note: this does not get into performance tweaks that are available for scaled joins.__
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
# MAGIC  __Last Update__ 01 DEC 2023 [Mosaic 0.3.12]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Mosaic
# MAGIC
# MAGIC > Mosaic framework is available via pip install and it comes with bindings for Python, SQL, Scala and R. The wheel file coming with pip installation is registering any necessary jars for other language support.

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
import pathlib
import requests
import warnings

warnings.simplefilter("ignore")

# COMMAND ----------

# MAGIC %md ## Setup Data

# COMMAND ----------

user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

data_dir = f"/tmp/mosaic/{user_name}" # <- DBFS
print(f"Initial data stored in '{data_dir}'")

# COMMAND ----------

# MAGIC %sh mount -l -t fuse

# COMMAND ----------

# MAGIC %md ### Download NYC Taxi Zones
# MAGIC
# MAGIC > Make sure we have New York City Taxi zone shapes available in our environment.

# COMMAND ----------

zone_dir = f"{data_dir}/taxi_zones"          # <- DBFS
zone_dir_fuse = f"/dbfs{zone_dir}"           # <- FUSE
dbutils.fs.mkdirs(zone_dir)

os.environ['ZONE_DIR_FUSE'] = zone_dir_fuse
print(f"ZONE_DIR_FUSE? '{zone_dir_fuse}'")

# COMMAND ----------

zone_url = 'https://data.cityofnewyork.us/api/geospatial/d3c5-ddgc?method=export&format=GeoJSON'

zone_fusepath = pathlib.Path(zone_dir_fuse) / 'nyc_taxi_zones.geojson'
if not zone_fuse_path.exists():
  req = requests.get(zone_url)
  with open(zone_fuse_path, 'wb') as f:
    f.write(req.content)
else:
  print(f"...skipping '{zone_fuse_path}', already exits.")

display(dbutils.fs.ls(zone_dir))

# COMMAND ----------

# MAGIC %md ### Initial Taxi Zone from GeoJSON [Polygons]
# MAGIC
# MAGIC > With the functionality Mosaic brings we can easily load GeoJSON files.

# COMMAND ----------

# MAGIC %python
# MAGIC # Note: Here we are using python for convenience since our
# MAGIC # data is on DBFS; after we create a temp view, can pick up
# MAGIC # in Spark SQL
# MAGIC (
# MAGIC   spark.read
# MAGIC     .option("multiline", "true")
# MAGIC     .format("json")
# MAGIC     .load(zone_dir)
# MAGIC     .select("type", F.explode(col("features")).alias("feature"))
# MAGIC     .select("type", col("feature.properties").alias("properties"), F.to_json(col("feature.geometry")).alias("json_geometry"))
# MAGIC     .withColumn("geometry", mos.st_aswkt(mos.st_geomfromgeojson("json_geometry")))
# MAGIC ).createOrReplaceTempView("neighbourhoods")

# COMMAND ----------

# MAGIC %sql select format_number(count(1), 0) as count from neighbourhoods

# COMMAND ----------

# MAGIC %sql
# MAGIC -- limiting for ipynb only
# MAGIC select * from neighbourhoods limit 1

# COMMAND ----------

# MAGIC %md ##  Compute some basic geometry attributes
# MAGIC
# MAGIC > Mosaic provides a number of functions for extracting the properties of geometries. Here are some that are relevant to Polygon geometries:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- limiting for ipynb only
# MAGIC select
# MAGIC   geometry,
# MAGIC   st_area(geometry) as calculatedArea,
# MAGIC   st_length(geometry) as calculatedLength
# MAGIC from neighbourhoods
# MAGIC limit 1

# COMMAND ----------

# MAGIC %md ### Initial Trips Data [Points]
# MAGIC
# MAGIC > We will load some Taxi trips data to represent point data; this data is coming from Databricks public datasets available in your environment. __Note: this is 1.6 billion trips as-is; while it is no problem to process this, to keep this to a quickstart level, we are going to use just 1/10th of 1% or ~1.6 million.__

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view trips as (
# MAGIC   select
# MAGIC     xxhash64(pickup_datetime, dropoff_datetime, pickup_geom, dropoff_geom) as row_id, *
# MAGIC   from (
# MAGIC     select
# MAGIC       trip_distance,
# MAGIC       pickup_datetime,
# MAGIC       dropoff_datetime,
# MAGIC       st_astext(st_point(pickup_longitude, pickup_latitude)) as pickup_geom,
# MAGIC       st_astext(st_point(dropoff_longitude, dropoff_latitude)) as dropoff_geom,
# MAGIC       total_amount
# MAGIC     from delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow`
# MAGIC     tablesample (0.1 percent) repeatable (123)
# MAGIC   )
# MAGIC );
# MAGIC
# MAGIC select format_number(count(1),0) as count from trips;

# COMMAND ----------

# MAGIC %md ## Spatial Joins
# MAGIC
# MAGIC > We can use Mosaic to perform spatial joins both with and without Mosaic indexing strategies. Indexing is very important when handling very different geometries both in size and in shape (ie. number of vertices).

# COMMAND ----------

# MAGIC %md ### Getting the optimal resolution
# MAGIC
# MAGIC > We can use Mosaic functionality to identify how to best index our data based on the data inside the specific dataframe. Selecting an appropriate indexing resolution can have a considerable impact on the performance.

# COMMAND ----------

from mosaic import MosaicFrame

neighbourhoods_mosaic_frame = MosaicFrame(spark.table("neighbourhoods"), "geometry")
optimal_resolution = neighbourhoods_mosaic_frame.get_optimal_resolution(sample_fraction=0.75)

print(f"Optimal resolution is {optimal_resolution}")

# COMMAND ----------

# MAGIC %md
# MAGIC > Not every resolution will yield performance improvements. By a rule of thumb it is always better to under-index than over-index - if not sure select a lower resolution. Higher resolutions are needed when we have very imbalanced geometries with respect to their size or with respect to the number of vertices. In such case indexing with more indices will considerably increase the parallel nature of the operations. You can think of Mosaic as a way to partition an overly complex row into multiple rows that have a balanced amount of computation each.

# COMMAND ----------

display(
  neighbourhoods_mosaic_frame.get_resolution_metrics(sample_rows=150)
)

# COMMAND ----------

# MAGIC %md ### Indexing using the optimal resolution
# MAGIC
# MAGIC > We will use mosaic sql functions to index our points data. Here we will use resolution 9, index resolution depends on the dataset in use.

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view tripsWithIndex as (
# MAGIC   select
# MAGIC     row_id, pickup_h3, dropoff_h3,
# MAGIC     * except(row_id, pickup_h3, dropoff_h3)
# MAGIC   from (
# MAGIC     select
# MAGIC       *,
# MAGIC       grid_pointascellid(pickup_geom, 9) as pickup_h3,
# MAGIC       grid_pointascellid(dropoff_geom, 9) as dropoff_h3,
# MAGIC       st_makeline(array(pickup_geom, dropoff_geom)) as trip_line
# MAGIC     from trips
# MAGIC   )
# MAGIC );
# MAGIC
# MAGIC select * from tripsWithIndex limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC > We will also index our neighbourhoods using a built in generator function.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- [1] We break down the original geometry in multiple smaller mosaic chips,
# MAGIC --     each with its own index.
# MAGIC -- [2] We don't need the original geometry any more, since we have broken
# MAGIC --     it down into smaller mosaic chips.
# MAGIC create or replace temp view neighbourhoodsWithIndex as (
# MAGIC   select
# MAGIC     * except(geometry, json_geometry),
# MAGIC     grid_tessellateexplode(geometry, 9) as mosaic_index
# MAGIC   from neighbourhoods
# MAGIC );
# MAGIC
# MAGIC -- notice the explode results in more rows
# MAGIC select format_number(count(1),0) as count from neighbourhoodsWithIndex

# COMMAND ----------

# MAGIC %sql
# MAGIC -- limiting for ipynb only
# MAGIC select * from neighbourhoodsWithIndex limit 1;

# COMMAND ----------

# MAGIC %md ### Performing the spatial join
# MAGIC
# MAGIC > We can now do spatial joins to both pickup and drop off zones based on geolocations in our datasets.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace temp view withPickupZone as (
# MAGIC   select
# MAGIC      trip_distance, pickup_geom, dropoff_geom, pickup_h3, dropoff_h3, pickup_zone, trip_line
# MAGIC   from tripsWithIndex
# MAGIC   join (
# MAGIC     select
# MAGIC       properties.zone as pickup_zone,
# MAGIC       mosaic_index
# MAGIC     from neighbourhoodsWithIndex
# MAGIC   )
# MAGIC   on mosaic_index.index_id == pickup_h3
# MAGIC   where mosaic_index.is_core or st_contains(mosaic_index.wkb, pickup_geom)
# MAGIC );
# MAGIC
# MAGIC -- limiting for ipynb only
# MAGIC select * from withPickupZone limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC > We can easily perform a similar join for the drop off location. __Note: in this case using `withPickupZone` from above as the left sid of the join.__

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view withDropoffZone as (
# MAGIC   select
# MAGIC      trip_distance, pickup_geom, dropoff_geom, pickup_h3, dropoff_h3, pickup_zone, dropoff_zone, trip_line
# MAGIC   from withPickupZone
# MAGIC   join (
# MAGIC     select
# MAGIC       properties.zone as dropoff_zone,
# MAGIC       mosaic_index
# MAGIC     from neighbourhoodsWithIndex
# MAGIC   )
# MAGIC   on mosaic_index.index_id == dropoff_h3
# MAGIC   where mosaic_index.is_core or st_contains(mosaic_index.wkb, dropoff_geom)
# MAGIC );
# MAGIC
# MAGIC -- limiting for ipynb only
# MAGIC select * from withDropoffZone limit 10

# COMMAND ----------

# MAGIC %md ## Visualise the results in Kepler
# MAGIC
# MAGIC > Mosaic abstracts interaction with Kepler in python through the use of the `%%mosaic_kepler` magic. When python is not the notebook language, you can prepend `%python` before the magic to make the switch.

# COMMAND ----------

# MAGIC %md _Here is the initial rendering with trip lines._

# COMMAND ----------

%%mosaic_kepler
withDropoffZone "pickup_h3" "h3" 5000

# COMMAND ----------

# MAGIC %md _Hint: you can toggle layers on/off and adjust properties._

# COMMAND ----------

# MAGIC %md ## Databricks Lakehouse can read / write most any data format
# MAGIC
# MAGIC > Here are [built-in](https://docs.databricks.com/en/external-data/index.html) formats as well as Mosaic [readers](https://databrickslabs.github.io/mosaic/api/api.html). __Note: best performance with Delta Lake format__, ref [Databricks](https://docs.databricks.com/en/delta/index.html) and [OSS](https://docs.delta.io/latest/index.html) docs for Delta Lake. Beyond built-in formats, Databricks is a platform on which you can install a wide variety of libraries, e.g. [1](https://docs.databricks.com/en/libraries/index.html#python-environment-management) | [2](https://docs.databricks.com/en/compute/compatibility.html) | [3](https://docs.databricks.com/en/init-scripts/index.html).
# MAGIC
# MAGIC Example of [reading](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.html?highlight=read#pyspark.sql.DataFrameReader) and [writing](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.html?highlight=pyspark%20sql%20dataframe%20writer#pyspark.sql.DataFrameWriter) a Spark DataFrame with Delta Lake format.
# MAGIC
# MAGIC ```
# MAGIC # - `write.format("delta")` is default in Databricks
# MAGIC # - can save to a specified path in the Lakehouse
# MAGIC # - can save as a table in the Databricks Metastore
# MAGIC df.write.save("<some_path>")
# MAGIC df.write.saveAsTable("<some_delta_table>")
# MAGIC ```
# MAGIC
# MAGIC Example of loading a Delta Lake Table as a Spark DataFrame.
# MAGIC
# MAGIC ```
# MAGIC # - `read.format("delta")` is default in Databricks
# MAGIC # - can load a specified path in the Lakehouse
# MAGIC # - can load a table in the Databricks Metastore
# MAGIC df.read.load("<some_path>")
# MAGIC df.table("<some_delta_table>")
# MAGIC ```
# MAGIC
# MAGIC More on [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html) in Databricks Lakehouse for Governing [Tables](https://docs.databricks.com/en/data-governance/unity-catalog/index.html#tables) and [Volumes](https://docs.databricks.com/en/data-governance/unity-catalog/index.html#volumes).
