# Databricks notebook source
# MAGIC %md # Analyze Coral Bleaching with Mosaic + GDAL
# MAGIC
# MAGIC > Read multiple NetCDFs using Mosaic and process through several performance-driving data engineering steps before rendering avg coral bleaching worldwide at h3 resolution `3`.
# MAGIC
# MAGIC __Notes:__
# MAGIC
# MAGIC <p/>
# MAGIC
# MAGIC * This notebook was updated for Mosaic [0.3.12](https://github.com/databrickslabs/mosaic/releases/tag/v_0.3.12) on DBR 12.2 LTS
# MAGIC * [GDAL](https://gdal.org/) supported in [Mosaic](https://databrickslabs.github.io/mosaic/index.html)
# MAGIC   * Install this GDAL [init script](https://github.com/databrickslabs/mosaic/blob/main/modules/python/gdal_package/databricks-mosaic-gdal/resources/scripts/mosaic-gdal-3.4.3-filetree-init.sh) (for DBR 12.2) on your cluster, see [[1](https://docs.databricks.com/en/init-scripts/cluster-scoped.html#use-cluster-scoped-init-scripts) | [2](https://databrickslabs.github.io/mosaic/usage/install-gdal.html)] for more.
# MAGIC * Recommend using an auto-scaling 2-8 worker cluster, doesn't need to be a large instance type but should use delta (aka disk) caching, more [here](https://docs.databricks.com/en/optimizations/disk-cache.html).
# MAGIC
# MAGIC ---  
# MAGIC __Last Update:__ 21 NOV 2023 [Mosaic 0.3.12]

# COMMAND ----------

# MAGIC %md ## Setup

# COMMAND ----------

# MAGIC %pip install "databricks-mosaic<0.4,>=0.3" --quiet

# COMMAND ----------

# -- configure AQE for more compute heavy operations
#  - choose option-1 or option-2 below, essential for REPARTITION!
# spark.conf.set("spark.databricks.optimizer.adaptive.enabled", False) # <- option-1: turn off completely for full control
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False) # <- option-2: just tweak partition management

# -- import databricks + spark functions

from pyspark.databricks.sql import functions as dbf
from pyspark.sql import functions as F
from pyspark.sql.functions import col

# -- setup mosaic
import mosaic as mos

mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

# -- other imports
import os

# COMMAND ----------

# MAGIC %md ## NetCDF Coral Bleaching Data
# MAGIC
# MAGIC > These files were uploaded from [Mosaic Test Resources](https://github.com/databrickslabs/mosaic/tree/main/src/test/resources/binary/netcdf-coral).
# MAGIC
# MAGIC __Hint:__ _Can also use [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html) to move files around, e.g. from your local machine._

# COMMAND ----------

# MAGIC %md _Download data [1x] into Workspace_
# MAGIC
# MAGIC > There are a few ways to do this; we will create a folder in our workspace; your path will look something like `/Workspace/Users/<your_email>/<path_to_dir>`. __Note: Spark cannot directly interact with Workspace files, so we will take an additional step after downloading, more [here](https://docs.databricks.com/en/files/workspace-interact.html#read-data-workspace-files).__ Workspace files are newer to Databricks and we want to make sure you get familiar with them.

# COMMAND ----------

ws_data = "/Workspace/Users/mjohns@databricks.com/All_Shared/mosaic_raster/NetCDF_Coral/data"

os.environ['WS_DATA'] = ws_data

# COMMAND ----------

# MAGIC %sh
# MAGIC # this is just in the workspace initially
# MAGIC mkdir -p $WS_DATA
# MAGIC ls -lh $WS_DATA/..

# COMMAND ----------

# MAGIC %sh 
# MAGIC # download all the nc files used
# MAGIC # - '-nc' means no clobber here
# MAGIC wget -P $WS_DATA -nc https://github.com/databrickslabs/mosaic/raw/main/src/test/resources/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220101.nc
# MAGIC wget -P $WS_DATA -nc https://github.com/databrickslabs/mosaic/raw/main/src/test/resources/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220102.nc
# MAGIC wget -P $WS_DATA -nc https://github.com/databrickslabs/mosaic/raw/main/src/test/resources/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220103.nc
# MAGIC wget -P $WS_DATA -nc https://github.com/databrickslabs/mosaic/raw/main/src/test/resources/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220104.nc
# MAGIC wget -P $WS_DATA -nc https://github.com/databrickslabs/mosaic/raw/main/src/test/resources/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220105.nc
# MAGIC wget -P $WS_DATA -nc https://github.com/databrickslabs/mosaic/raw/main/src/test/resources/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220106.nc
# MAGIC wget -P $WS_DATA -nc https://github.com/databrickslabs/mosaic/raw/main/src/test/resources/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220107.nc
# MAGIC wget -P $WS_DATA -nc https://github.com/databrickslabs/mosaic/raw/main/src/test/resources/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220108.nc
# MAGIC wget -P $WS_DATA -nc https://github.com/databrickslabs/mosaic/raw/main/src/test/resources/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220109.nc
# MAGIC wget -P $WS_DATA -nc https://github.com/databrickslabs/mosaic/raw/main/src/test/resources/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220110.nc

# COMMAND ----------

# MAGIC %md _For simplicity (and since we are running DBR 12.2), we are going to copy from the Workspace folder to DBFS, but this is all shifting with Unity Catalog (more [here](https://docs.databricks.com/en/dbfs/unity-catalog.html))._ __Note: [DBFS](https://docs.databricks.com/en/dbfs/dbfs-root.html), and more recent [Volumes](https://docs.databricks.com/en/data-governance/unity-catalog/index.html#volumes), are FUSE mounted to the cluster nodes, looking like a local path.__

# COMMAND ----------

dbfs_data = "/home/mjohns@databricks.com/datasets/netcdf-coral"
dbfs_data_fuse = f"/dbfs{dbfs_data}"
os.environ['DBFS_DATA'] = dbfs_data
os.environ['DBFS_DATA_FUSE'] = dbfs_data_fuse

# COMMAND ----------

# MAGIC %sh 
# MAGIC # copy from workspace
# MAGIC # - for spark / distributed work
# MAGIC mkdir -p $DBFS_DATA_FUSE
# MAGIC cp -r $WS_DATA/* $DBFS_DATA_FUSE
# MAGIC ls -lh $DBFS_DATA_FUSE

# COMMAND ----------

# MAGIC %md ## Read NetCDFs with Spark
# MAGIC
# MAGIC > Uses Mosaic [GDAL readers](https://databrickslabs.github.io/mosaic/api/raster-format-readers.html#raster-format-readers). __Note: starting with Mosaic 0.3.12, the 'tile' column is populated and is used by various `rst_` functions.__

# COMMAND ----------

df = (
  spark
    .read.format("gdal")
      .option("driverName", "NetCDF")
    .load(dbfs_data)
)
print(f"count? {df.count():,}")
df.display()

# COMMAND ----------

# MAGIC %md __Let's work with the "bleaching_alert_area" subdataset.__
# MAGIC
# MAGIC > We are using `rst_subdataset` which uses the (new) 'tile' column, more [here](https://databrickslabs.github.io/mosaic/api/raster-functions.html#rst-getsubdataset).

# COMMAND ----------

df_bleach = (
  df
    .repartition(df.count(), "tile")
    .select(
      mos
        .rst_getsubdataset("tile", F.lit("bleaching_alert_area"))
        .alias("tile")
    )
)
print(f"count? {df_bleach.count():,}")
df_bleach.display()

# COMMAND ----------

# MAGIC %md ## Subdivide tiles from subdataset column to max of 8MB
# MAGIC
# MAGIC > While this is optional for smaller data, we want to demonstrate how you can master tiling at any scale. Let's use [rst_subdivide](https://databrickslabs.github.io/mosaic/api/raster-functions.html#rst-subdivide) to ensure we have tiles no larger than 8MB.

# COMMAND ----------

df_subdivide = (
  df_bleach
    .repartition(df_bleach.count(), "tile") # <- repartition important!
    .select(
      mos
        .rst_subdivide(col("tile"), F.lit(8))
      .alias("tile")
    )
)
print(f"count? {df_subdivide.count():,}")   # <- go from 10 to 40 tiles
df_subdivide.display()

# COMMAND ----------

# MAGIC %md ## ReTile tiles from subdataset to 600x600 pixels
# MAGIC
# MAGIC > While this is optional for smaller data, we want to demonstrate how you can master tiling at any scale. Let's use [rst_retile](https://databrickslabs.github.io/mosaic/api/raster-functions.html#rst-retile) to ensure we have even data and drive more parallelism.

# COMMAND ----------

df_retile = (
  df_subdivide
    .repartition(df_subdivide.count(), "tile") # <- repartition important!
    .select(
      mos
        .rst_retile(col("tile"), F.lit(600), F.lit(600))
      .alias("tile")
    )
)
print(f"count? {df_retile.count():,}")        # <- go from 40 to 463 tiles
df_retile.limit(10).display()

# COMMAND ----------

# MAGIC %md ## Render Raster to H3 Results
# MAGIC
# MAGIC > Use [rst_rastertogridavg](https://databrickslabs.github.io/mosaic/api/raster-functions.html#rst-rastertogridavg) to tessellate to grid (default is h3) and provide the average measure for the resolution chosen (in this case resolution `3`); also, creates a temp view & renders with Kepler.gl.
# MAGIC
# MAGIC Data ultimately looks something like the following:
# MAGIC
# MAGIC | h3 | measure |
# MAGIC | --- | ------- |
# MAGIC | 593176490141548543 | 0 |
# MAGIC | 593386771740360703 | 2.0113207547169814 |
# MAGIC | 593308294097928191 | 0 |
# MAGIC | 593825202001936383 | 0.015432098765432098 |
# MAGIC | 593163914477305855 | 2.008650519031142 |
# MAGIC
# MAGIC __Hint: zoom back out once rendered; also, verify the `.contains()` string is actually in the data. Also, this can take a few minutes to run, recommend a few nodes (min. 3 to say 8) in your cluster to speed up processing__

# COMMAND ----------

# here is the initial structure
# - notice the array nesting, which we will handle
#   by exploding 2x
display (
  df_retile
    .limit(5)
    .select(
     mos.rst_rastertogridavg("tile", F.lit(3))
     .alias("grid_avg")
    )
)

# COMMAND ----------

# MAGIC %md _Prepare a View for rendering with Kepler + other analysis._
# MAGIC
# MAGIC > This generates 241,486 rows (row per cellid at h3 resolution `3`).

# COMMAND ----------

# create view "to_display"
# - you could also write to Delta Lake 
#   at any point to avoid recomputing
(
    df_retile
    .repartition(df_retile.count(), "tile")
        .select(mos.rst_rastertogridavg("tile", F.lit(3)).alias("grid_avg"))
    .select(F.explode(col("grid_avg")).alias("grid_avg")) # <- explode-1 of 2d array
    .select(F.explode(col("grid_avg")).alias("grid_avg")) # <- explode-2 of 2d array
    .select(
        F.col("grid_avg").getItem("cellID").alias("h3"),      # <- h3 cellid
        F.col("grid_avg").getItem("measure").alias("measure") # <- coral bleaching
    )
    .createOrReplaceTempView("to_display")
)

# optional: can work with the view in sql
# - you would probably want to write to delta lake 
#   to avoid recompute
# print(f"""count? {spark.table("to_display").count()}""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- optional: can work with the view in sql
# MAGIC -- you would probably want to write to delta lake 
# MAGIC -- to avoid recompute
# MAGIC -- select * from to_display

# COMMAND ----------

# MAGIC %md _Render with Kepler.gl via Mosaic magic._
# MAGIC
# MAGIC > Hint: zoom out within the map viewport to see all available data rendered.

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC "to_display" "h3" "h3" 250_000

# COMMAND ----------

# MAGIC %md _Hint: scroll out to see the full results._

# COMMAND ----------

# MAGIC %md ### Databricks Lakehouse can read / write most any data format
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
