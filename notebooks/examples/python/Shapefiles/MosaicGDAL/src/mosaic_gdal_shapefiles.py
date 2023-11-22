# Databricks notebook source
# MAGIC %md # Mosaic + GDAL Shapefile Example
# MAGIC
# MAGIC > Download Shapefile(s) from https://www2.census.gov/geo/tiger/TIGER_RD18/LAYER/ADDR/
# MAGIC
# MAGIC __Artifacts Generated__
# MAGIC <p/>
# MAGIC
# MAGIC 1. Volume - `<catalog>.<schema>.census_data/address_block_shapefiles`
# MAGIC 1. Table - `<catalog>.<schema>.ga_address_block`
# MAGIC
# MAGIC ---   
# MAGIC __Last Update: 22 NOV 2023 [Mosaic 0.3.12]__

# COMMAND ----------

# MAGIC %md ## Setup
# MAGIC <p/>
# MAGIC
# MAGIC 1. Import Databricks columnar functions (including H3) for DBR / DBSQL Photon with `from pyspark.databricks.sql.functions import *`
# MAGIC 2. To use Databricks Labs [Mosaic](https://databrickslabs.github.io/mosaic/index.html) library for geospatial data engineering, analysis, and visualization functionality:
# MAGIC   * Install with `%pip install databricks-mosaic`
# MAGIC   * Import and use with the following:
# MAGIC   ```
# MAGIC   import mosaic as mos
# MAGIC   mos.enable_mosaic(spark, dbutils)
# MAGIC   ```
# MAGIC <p/>
# MAGIC
# MAGIC 3. For Mosaic + GDAL (used for SHP reading)
# MAGIC   * Follow instructions for your version [here](https://databrickslabs.github.io/mosaic/usage/install-gdal.html)
# MAGIC   * then additionally call the following in the notebook 
# MAGIC   ```
# MAGIC   mos.enable_gdal(spark)
# MAGIC   ```
# MAGIC <p/>
# MAGIC
# MAGIC 4. To use [KeplerGl](https://kepler.gl/) OSS library for map layer rendering:
# MAGIC   * Already installed with Mosaic, use `%%mosaic_kepler` magic [[Mosaic Docs](https://databrickslabs.github.io/mosaic/usage/kepler.html)]
# MAGIC   * Import with `from keplergl import KeplerGl` to use directly
# MAGIC
# MAGIC __Notes:__
# MAGIC
# MAGIC If you hit `H3_NOT_ENABLED` [[docs](https://docs.databricks.com/error-messages/h3-not-enabled-error-class.html#h3_not_enabled-error-class)]
# MAGIC
# MAGIC > `h3Expression` is disabled or unsupported. Consider enabling Photon or switch to a tier that supports H3 expressions. [[AWS](https://www.databricks.com/product/aws-pricing) | [Azure](https://azure.microsoft.com/en-us/pricing/details/databricks/) | [GCP](https://www.databricks.com/product/gcp-pricing)]
# MAGIC
# MAGIC If you have trouble with Volume access:
# MAGIC
# MAGIC * For Mosaic 0.3 series (< DBR 13)     - you can copy resources to DBFS as a workaround
# MAGIC * For Mosaic 0.4 series (DBR 13.3 LTS) - you will need to either copy resources to DBFS or setup for Unity Catalog + Shared Access which will involve your workspace admin. Instructions, as updated, will be [here](https://databrickslabs.github.io/mosaic/usage/install-gdal.html).

# COMMAND ----------

# MAGIC %pip install "databricks-mosaic<0.4,>=0.3" --quiet # <- Mosaic 0.3 series
# MAGIC # %pip install "databricks-mosaic<0.5,>=0.4" --quiet # <- Mosaic 0.4 series (as available)

# COMMAND ----------

# -- configure AQE for more compute heavy operations
#  - choose option-1 or option-2 below, essential for REPARTITION!
# spark.conf.set("spark.databricks.optimizer.adaptive.enabled", False) # <- option-1: turn off completely for full control
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False) # <- option-2: just tweak partition management
spark.conf.set("spark.sql.shuffle.partitions", 10_000)                 # <-- default is 200

# -- import databricks + spark functions
from pyspark.databricks.sql import functions as dbf
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.types import *

# -- setup mosaic
import mosaic as mos

mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

# --other imports
import os
import warnings

warnings.simplefilter("ignore")

# COMMAND ----------

# MAGIC %md __Configure Database + Username__
# MAGIC
# MAGIC > Note: Adjust this to your own specified [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/admin-privileges.html#managing-unity-catalog-metastores) Schema.

# COMMAND ----------

catalog_name = "mjohns"
db_name = "census"

sql(f"use catalog {catalog_name}")
sql(f"use schema {db_name}")

# COMMAND ----------

# %sql show tables

# COMMAND ----------

# MAGIC %md __Setup `ETL_DIR` + `ETL_DIR_FUSE`__
# MAGIC
# MAGIC > Note: Adjust this to your own specified [Volume](https://docs.databricks.com/en/ingestion/add-data/upload-to-volume.html#upload-files-to-a-unity-catalog-volume) (under a schema).

# COMMAND ----------

ETL_DIR = f'/Volumes/{catalog_name}/{db_name}/census_data/address_block_shapefiles'
os.environ['ETL_DIR'] = ETL_DIR

dbutils.fs.mkdirs(ETL_DIR)
print(f"...ETL_DIR: '{ETL_DIR}' (create)")

# COMMAND ----------

ls $ETL_DIR/..

# COMMAND ----------

# MAGIC %md ## Get All GA Addresses (Shapefiles)
# MAGIC <p/>
# MAGIC
# MAGIC * Look for pattern https://www2.census.gov/geo/tiger/TIGER_RD18/LAYER/ADDRFEAT/tl_rd22_13*.zip (13 is GA number)

# COMMAND ----------

state_num = "13"

# COMMAND ----------

# MAGIC %md __Make `address_features` directory.__

# COMMAND ----------

dbutils.fs.mkdirs(f"{ETL_DIR}/address_features")

# COMMAND ----------

# MAGIC %md ### Get List of Shapefile ZIPs

# COMMAND ----------

# MAGIC %sh 
# MAGIC echo "$PWD"
# MAGIC wget -O address_features.txt -nc "https://www2.census.gov/geo/tiger/TIGER_RD18/LAYER/ADDRFEAT/"

# COMMAND ----------

dbutils.fs.cp("file:/databricks/driver/address_features.txt", ETL_DIR)
display(dbutils.fs.ls(ETL_DIR))

# COMMAND ----------

# MAGIC %md __Figure out which rows are within the `<table>` tag and extract the filenames.__
# MAGIC
# MAGIC > Since this is all in one file being read on one node, get consistent ordered id for `row_num` (not always true).

# COMMAND ----------

tbl_start_row = (
  spark.read.text(f"{ETL_DIR}/address_features.txt")
  .withColumn("row_num", F.monotonically_increasing_id())
  .withColumn("tbl_start_row", F.trim("value") == '<table>')
  .filter("tbl_start_row = True")
  .select("row_num")
).collect()[0][0]

tbl_end_row = (
  spark.read.text(f"{ETL_DIR}/address_features.txt")
  .withColumn("row_num", F.monotonically_increasing_id())
  .withColumn("tbl_end_row", F.trim("value") == '</table>')
  .filter("tbl_end_row = True")
  .select("row_num")
).collect()[0][0]

print(f"tbl_start_row: {tbl_start_row}, tbl_end_row: {tbl_end_row}")

# COMMAND ----------

state_files = [r[1] for r in (
  spark.read.text(f"{ETL_DIR}/address_features.txt")
  .withColumn("row_num", F.monotonically_increasing_id())
    .filter(f"row_num > {tbl_start_row}")
    .filter(f"row_num < {tbl_end_row}")
  .withColumn("href_start", F.substring_index("value", 'href="', -1))
  .withColumn("href", F.substring_index("href_start", '">', 1))
    .filter(col("href").startswith(f"tl_rd22_{state_num}")) 
  .select("row_num","href")
).collect()]

print(f"len state files? {len(state_files):,}")
state_files[:5]

# COMMAND ----------

# MAGIC %md ### Download Shapefile ZIPs (159)
# MAGIC
# MAGIC > Could do this in parallel, but keeping on just driver for now so as to not overload Census server with requests.
# MAGIC
# MAGIC __Note: writing locally to driver, then copying to volume with `dbutils`.__

# COMMAND ----------

import pathlib
import requests

vol_path = pathlib.Path(f"{ETL_DIR}/address_features")
local_path = pathlib.Path(f"address_features")
local_path.mkdir(parents=True, exist_ok=True)

for idx,f in enumerate(state_files):
  idx_str = str(idx).rjust(4)
  
  vol_file = vol_path / f
  if not vol_file.exists():
    local_file = local_path / f 
    print(f"{idx_str} --> '{f}'")
    req = requests.get(f'https://www2.census.gov/geo/tiger/TIGER_RD18/LAYER/ADDRFEAT/{f}')
    with open(local_file, 'wb') as f:
      f.write(req.content)
  else:
    print(f"{idx_str} --> '{f}' exists...skipping")

# COMMAND ----------

dbutils.fs.cp("file:/databricks/driver/address_features", f"{ETL_DIR}/address_features", recurse=True)

# COMMAND ----------

# MAGIC %sh
# MAGIC # avoid list all files
# MAGIC ls -lh $ETL_DIR/address_features | head -5
# MAGIC echo "..."
# MAGIC ls -lh $ETL_DIR/address_features | tail -5

# COMMAND ----------

# MAGIC %md __Note:__ Showing DBFS based processing [Volumes](https://docs.databricks.com/en/sql/language-manual/sql-ref-volumes.html) for access, though you could skip this if all setup with Unity Catalog + Shared Access clusters.

# COMMAND ----------

# - change to your preferred DBFS path
ETL_DBFS_DIR = "/home/mjohns@databricks.com/datasets/census/address_features"
os.environ['ETL_DBFS_DIR'] = ETL_DBFS_DIR
dbutils.fs.mkdirs(ETL_DBFS_DIR)

# COMMAND ----------

dbutils.fs.cp(f"{ETL_DIR}/address_features", ETL_DBFS_DIR, recurse=True)
display(dbutils.fs.ls(ETL_DBFS_DIR)[:5]) # <- just showing the first 5 for ipynb

# COMMAND ----------

# MAGIC %md ### Test Render with Kepler
# MAGIC
# MAGIC > Just rendering the first file `tl_rd22_13001_addrfeat.zip` for an example. 

# COMMAND ----------

df_kepler = (
  mos.read()
    .format("multi_read_ogr")
    .option("vsizip", "true")
    .option("asWKB", "false")
    .load(f"dbfs:{ETL_DBFS_DIR}/tl_rd22_13001_addrfeat.zip")
  .withColumn("geom", mos.st_geomfromwkt("geom_0"))
  .withColumn("is_valid", mos.st_isvalid("geom"))
  .selectExpr(
    "fullname", "lfromhn", "ltohn", "zipl", "rfromhn", "rtohn", "zipr",
    "geom_0 as geom_wkt", "is_valid"
  )
)
print(f"count? {df_kepler.count():,}, num invalid? {df_kepler.filter('is_valid = False').count():,}")
df_kepler.limit(1).display() # <- limiting for ipynb only

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_kepler "geom_wkt" "geometry" 10_000 

# COMMAND ----------

# MAGIC %md ## Shapefiles to Delta Lake
# MAGIC
# MAGIC > Will use Mosaic + GDAL to read the ShapeFiles and write as Delta Lake to DBFS
# MAGIC
# MAGIC __Focus on `ADDRFEAT` (Address Feature) for both geometries and address ranges.__

# COMMAND ----------

# MAGIC %md __Assess `ST_Transform` to 4326 (from 4269)__
# MAGIC
# MAGIC > See that 'geom_0_srid' is 4269, so use `st_setsrid` and `st_transform` to standardize to 4326, more [here](https://databrickslabs.github.io/mosaic/usage/grid-indexes-bng.html#coordinate-reference-system). _This just uses one file to demonstrate the transform initially, full data is transformed later._
# MAGIC
# MAGIC __Note:__ _This pattern will shift to avoid Mosaic internal geometry in Mosaic 0.4 series._

# COMMAND ----------

df_test = (
  mos.read()
    .format("multi_read_ogr")
      .option("vsizip", "true")
      .option("asWKB", "false")
    .load(f"dbfs:{ETL_DBFS_DIR}/tl_rd22_13001_addrfeat.zip")
)
print(f"count? {df_test.count():,}")
df_test.limit(1).display() # <- limiting for ipynb only

# COMMAND ----------

df_trans_test = (
  mos.read()
    .format("multi_read_ogr")
      .option("vsizip", "true")
      .option("asWKB", "false")
    .load(f"dbfs:{ETL_DBFS_DIR}/tl_rd22_13001_addrfeat.zip")
    .withColumn("geom_4269", mos.st_geomfromwkt("geom_0"))
    .withColumn("geom_4269", mos.st_setsrid("geom_4269", F.lit(4269)))
    .withColumn("can_coords_from_4269", mos.st_hasvalidcoordinates("geom_4269", F.lit("EPSG:4326"), F.lit('reprojected_bounds')))
    .withColumn("geom", mos.st_transform("geom_4269", F.lit(4326)))
    .withColumn("is_coords_4326", mos.st_hasvalidcoordinates("geom", F.lit("EPSG:4326"), F.lit('bounds')))
    .withColumn("geom_wkt", mos.st_astext("geom"))
    .withColumn("is_valid", mos.st_isvalid("geom_wkt"))
)
print(f"count? {df_trans_test.count():,}")
df_trans_test.limit(1).display() # <- limiting for ipynb only

# COMMAND ----------

num_shapefiles = len(dbutils.fs.ls(ETL_DBFS_DIR))
num_shapefiles

# COMMAND ----------

_df = (
  mos.read()
    .format("multi_read_ogr")
      .option("vsizip", "true")
      .option("asWKB", "false")
      .load(f"dbfs:{ETL_DBFS_DIR}/")
    .repartition(num_shapefiles, F.rand())
      .withColumn("geom_4269", mos.st_geomfromwkt("geom_0"))
      .withColumn("geom_4269", mos.st_setsrid("geom_4269", F.lit(4269)))
      .withColumn("geom", mos.st_transform("geom_4269", F.lit(4326)))
      .withColumn("geom_wkt", mos.st_astext("geom"))
      .withColumn("is_valid", mos.st_isvalid("geom_wkt"))
    .selectExpr(
      "* except(geom_0, geom_4269, geom, geom_wkt, is_valid)",
      "geom_wkt", "is_valid"
    )
)

## -- wait until write to delta, will be faster --
# print(f"""count? {_df.count():,}, num invalid? {_df.filter("is_valid = False").count():,}""")
# _df.display()

# COMMAND ----------

# MAGIC %md ### Write to Delta Lake
# MAGIC
# MAGIC > We are saving as a managed table named 'ga_address_block'.

# COMMAND ----------

(
  _df
    .write
      .mode("overwrite")
      .option("mergeSchema", "true")
    .saveAsTable(f"{catalog_name}.{db_name}.ga_address_block")
)

df_address = spark.table(f"{catalog_name}.{db_name}.ga_address_block")

print(f"count? {df_address.count():,}, num invalid? {df_address.filter('is_valid = False').count():,}")
df_address.limit(1).display()

# COMMAND ----------

# MAGIC %md ## Final Sanity Check

# COMMAND ----------

# MAGIC %sql show tables

# COMMAND ----------

# MAGIC %sql select * from ga_address_block limit 1
