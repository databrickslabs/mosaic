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
# MAGIC  __Last Update__ 28 NOV 2023 [Mosaic 0.3.12]

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

data_dir = f"/tmp/mosaic/{user_name}"
print(f"Initial data stored in '{data_dir}'")

# COMMAND ----------

# MAGIC %md ### Download NYC Taxi Zones
# MAGIC
# MAGIC > Make sure we have New York City Taxi zone shapes available in our environment.

# COMMAND ----------

zone_dir = f"{data_dir}/taxi_zones"
zone_dir_fuse = f"/dbfs{zone_dir}"
dbutils.fs.mkdirs(zone_dir)

os.environ['ZONE_DIR_FUSE'] = zone_dir_fuse
print(f"ZONE_DIR_FUSE? '{zone_dir_fuse}'")

# COMMAND ----------

zone_url = 'https://data.cityofnewyork.us/api/geospatial/d3c5-ddgc?method=export&format=GeoJSON'

zone_fuse_path = pathlib.Path(zone_dir_fuse) / 'nyc_taxi_zones.geojson'
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

neighbourhoods = (
  spark.read
    .option("multiline", "true")
    .format("json")
    .load(zone_dir)
    .select("type", explode(col("features")).alias("feature"))
    .select("type", col("feature.properties").alias("properties"), to_json(col("feature.geometry")).alias("json_geometry"))
    .withColumn("geometry", mos.st_aswkt(mos.st_geomfromgeojson("json_geometry")))
)

print(f"count? {neighbourhoods.count():,}")
neighbourhoods.limit(1).display() # <- limiting for ipynb only

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Compute some basic geometry attributes

# COMMAND ----------

# MAGIC %md
# MAGIC Mosaic provides a number of functions for extracting the properties of geometries. Here are some that are relevant to Polygon geometries:

# COMMAND ----------

display(
  neighbourhoods
    .withColumn("calculatedArea", mos.st_area(col("geometry")))
    .withColumn("calculatedLength", mos.st_length(col("geometry")))
    # Note: The unit of measure of the area and length depends on the CRS used.
    # For GPS locations it will be square radians and radians
    .select("geometry", "calculatedArea", "calculatedLength")
    .limit(3)
    .show() # <- limiting + show for ipynb only
)

# COMMAND ----------

# MAGIC %md ### Initial Trips Data [Points]
# MAGIC
# MAGIC > We will load some Taxi trips data to represent point data; this data is coming from Databricks public datasets available in your environment. __Note: this is 1.6 billion trips as-is; while it is no problem to process this, to keep this to a quickstart level, we are going to use just 1/10th of 1% or ~1.6 million.__

# COMMAND ----------

trips = (
  spark.table("delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow`")
  # - .1% sample
  .sample(.001)
    .drop("vendorId", "rateCodeId", "store_and_fwd_flag", "payment_type")
    .withColumn("pickup_geom", mos.st_astext(mos.st_point(col("pickup_longitude"), col("pickup_latitude"))))
    .withColumn("dropoff_geom", mos.st_astext(mos.st_point(col("dropoff_longitude"), col("dropoff_latitude"))))
  # - adding a row id
  .selectExpr(
    "xxhash64(pickup_datetime, dropoff_datetime, pickup_geom, dropoff_geom) as row_id", "*"
  )
)
print(f"count? {trips.count():,}")
trips.limit(10).display() # <- limiting for ipynb only

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

neighbourhoods_mosaic_frame = MosaicFrame(neighbourhoods, "geometry")
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

tripsWithIndex = (
  trips
    .withColumn("pickup_h3", mos.grid_pointascellid(col("pickup_geom"), lit(optimal_resolution)))
    .withColumn("dropoff_h3", mos.grid_pointascellid(col("dropoff_geom"), lit(optimal_resolution)))
  # - beneficial to have index in first 32 table cols
  .selectExpr(
    "row_id", "pickup_h3", "dropoff_h3", "* except(row_id, pickup_h3, dropoff_h3)"
  )
)
display(tripsWithIndex.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC > We will also index our neighbourhoods using a built in generator function.

# COMMAND ----------

neighbourhoodsWithIndex = (
  neighbourhoods
    # We break down the original geometry in multiple smaller mosaic chips, each with its
    # own index
    .withColumn(
      "mosaic_index",
      mos.grid_tessellateexplode(col("geometry"), lit(optimal_resolution))
    )

    # We don't need the original geometry any more, since we have broken it down into
    # Smaller mosaic chips.
    .drop("json_geometry", "geometry")
)

print(f"count? {neighbourhoodsWithIndex.count():,}") # <- notice the explode results in more rows
neighbourhoodsWithIndex.limit(5).show()              # <- limiting + show for ipynb only

# COMMAND ----------

# MAGIC %md ### Performing the spatial join
# MAGIC
# MAGIC > We can now do spatial joins to both pickup and drop off zones based on geolocations in our datasets.

# COMMAND ----------

pickupNeighbourhoods = neighbourhoodsWithIndex.select(col("properties.zone").alias("pickup_zone"), col("mosaic_index"))

withPickupZone = (
  tripsWithIndex.join(
    pickupNeighbourhoods,
    tripsWithIndex["pickup_h3"] == pickupNeighbourhoods["mosaic_index.index_id"]
  ).where(
    # If the borough is a core chip (the chip is fully contained within the geometry), then we do not need
    # to perform any intersection, because any point matching the same index will certainly be contained in
    # the borough. Otherwise we need to perform an st_contains operation on the chip geometry.
    col("mosaic_index.is_core") | mos.st_contains(col("mosaic_index.wkb"), col("pickup_geom"))
  ).select(
    "trip_distance", "pickup_geom", "pickup_zone", "dropoff_geom", "pickup_h3", "dropoff_h3"
  )
)

display(withPickupZone.limit(10)) # <- limiting for ipynb only

# COMMAND ----------

# MAGIC %md
# MAGIC > We can easily perform a similar join for the drop off location. __Note: in this case using `withPickupZone` from above as the left sid of the join.__

# COMMAND ----------

dropoffNeighbourhoods = neighbourhoodsWithIndex.select(col("properties.zone").alias("dropoff_zone"), col("mosaic_index"))

withDropoffZone = (
  withPickupZone.join(
    dropoffNeighbourhoods,
    withPickupZone["dropoff_h3"] == dropoffNeighbourhoods["mosaic_index.index_id"]
  ).where(
    col("mosaic_index.is_core") | mos.st_contains(col("mosaic_index.wkb"), col("dropoff_geom"))
  ).select(
    "trip_distance", "pickup_geom", "pickup_zone", "dropoff_geom", "dropoff_zone", "pickup_h3", "dropoff_h3"
  )
  .withColumn("trip_line", mos.st_astext(mos.st_makeline(array(mos.st_geomfromwkt(col("pickup_geom")), mos.st_geomfromwkt(col("dropoff_geom"))))))
)

display(withDropoffZone.limit(10)) # <- limiting for ipynb only

# COMMAND ----------

# MAGIC %md ## Visualise the results in Kepler
# MAGIC
# MAGIC > Mosaic abstracts interaction with Kepler in python through the use of the `%%mosaic_kepler` magic. When python is not the notebook language, you can prepend `%python` before the magic to make the switch.

# COMMAND ----------

%%mosaic_kepler
withDropoffZone "pickup_h3" "h3" 5000

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
