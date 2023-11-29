# Databricks notebook source
# MAGIC %md # GeoPandas Shapefiles UDF Example
# MAGIC
# MAGIC > These are Census address blocks; download Shapefile(s) from https://www2.census.gov/geo/tiger/TIGER_RD18/LAYER/ADDR/
# MAGIC
# MAGIC __Artifacts Generated__
# MAGIC <p/>
# MAGIC
# MAGIC 1. Volume - `<catalog>.<schema>.census_data/address_block_shapefiles`
# MAGIC 1. Table - `<catalog>.<schema>.shape_address_block`
# MAGIC
# MAGIC ---  
# MAGIC __Last Update:__ 22 NOV 2023 [Mosaic 0.3.12]

# COMMAND ----------

# MAGIC %md ## Setup
# MAGIC <p/>
# MAGIC
# MAGIC 1. [GeoPandas](https://pypi.org/project/geopandas/) - used for Shapefile reading and rendering 
# MAGIC 1. [Contextily](https://pypi.org/project/contextily/) - used to add basemap to GeoPandas, supports WGS84 (4326) and Spheric Mercator (3857)
# MAGIC 1. Import Databricks columnar functions (including H3) for DBR / DBSQL Photon with `from pyspark.databricks.sql.functions import *`
# MAGIC
# MAGIC __Note: If you hit `H3_NOT_ENABLED` [[docs](https://docs.databricks.com/error-messages/h3-not-enabled-error-class.html#h3_not_enabled-error-class)]__
# MAGIC
# MAGIC > `h3Expression` is disabled or unsupported. Consider enabling Photon or switch to a tier that supports H3 expressions. [[AWS](https://www.databricks.com/product/aws-pricing) | [Azure](https://azure.microsoft.com/en-us/pricing/details/databricks/) | [GCP](https://www.databricks.com/product/gcp-pricing)]
# MAGIC
# MAGIC __Note:__ _Recommend run on DBR 14.1+ for better [Volumes](https://docs.databricks.com/en/sql/language-manual/sql-ref-volumes.html) support._

# COMMAND ----------

# MAGIC %pip install geopandas contextily --quiet

# COMMAND ----------

from pyspark.databricks.sql import functions as dbf
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.types import *

import contextily as cx
import fiona
import geopandas as gpd
import os
import pandas as pd

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 10_000)                        # <-- default is 200

# https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution
# spark.conf.set("spark.databricks.optimizer.adaptive.enabled", True)         # <-- default is true [nuclear option]
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False)        # <-- default is true [softer option]

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
# MAGIC > Note: Adjust this to your own specified [Volume](https://docs.databricks.com/en/ingestion/add-data/upload-to-volume.html#upload-files-to-a-unity-catalog-volume) (under a schema). _You must already have setup the Volume path._

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

# MAGIC %md ### Test Render with GeoPandas
# MAGIC
# MAGIC > Just rendering the first file `tl_rd22_13001_addrfeat.zip` for an example. 

# COMMAND ----------

# %sh
# - Can copy locally to driver (but don't have to)
# mkdir -p $PWD/address_features
# cp $ETL_DIR/address_features/tl_rd22_13001_addrfeat.zip $PWD/address_features
# ls -lh $PWD/address_features

# COMMAND ----------

# MAGIC %md _Get layer information_
# MAGIC
# MAGIC > Fiona is a dependency of GeoPandas.

# COMMAND ----------

fiona.listlayers(f"zip://{ETL_DIR}/address_features/tl_rd22_13001_addrfeat.zip") # <- 'zip://' is required here

# COMMAND ----------

#options: driver='shapefile', layer=0; also, 'zip://' is optional
gdf = gpd.read_file(f"{ETL_DIR}/address_features/tl_rd22_13001_addrfeat.zip")
print(f'rows? {gdf.shape[0]:,}, cols? {gdf.shape[1]}')
gdf.head()

# COMMAND ----------

# MAGIC %md _Map Rendering_
# MAGIC
# MAGIC > Convert to WGS84 (EPSG=4326) for rendering + this is recommended as baseline for all data layers.

# COMMAND ----------

gdf.crs

# COMMAND ----------

gdf_4326 = gdf.to_crs(epsg=4326)
gdf_4326.crs.to_string() # <- will be used with contextily

# COMMAND ----------

ax = gdf_4326.plot(column='ZIPL', cmap=None, legend=True, figsize=(20, 20), alpha=0.5, edgecolor="k")
cx.add_basemap(ax, zoom='auto', crs=gdf_4326.crs.to_string()) # <- specify crs!

# COMMAND ----------

# MAGIC %md ## Shapefiles to Delta Lake
# MAGIC
# MAGIC > Will use GeoPandas to read the ShapeFiles and write as Delta Table
# MAGIC
# MAGIC __Focus on `ADDRFEAT` (Address Feature) for both geometries and address ranges.__

# COMMAND ----------

num_shapefiles = len(dbutils.fs.ls(f"{ETL_DIR}/address_features"))
num_shapefiles

# COMMAND ----------

# MAGIC %md __[1] Define the UDF function.__
# MAGIC
# MAGIC > This will be invoked with [applyInPandas](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.applyInPandas.html?highlight=applyinpandas).

# COMMAND ----------

gdf_4326.columns

# COMMAND ----------

def geopandas_read(pdf:pd.DataFrame) -> pd.DataFrame:
  """
  Read using geopandas; recommend using `repartition`
  in caller to drive parallelism.
  - 'path' field assumed to be a Volume path,
    which is automatically FUSE mounted
  - layer_num is either field 'layer_num', if present
    or defaults to 0
  - standardizes to CRS=4326
  """
  pdf_arr = []

  # --- iterate over pdf ---
  for index, row in pdf.iterrows():
    # [1] read 'path' + 'layer_num'
    layer_num = 0
    if 'layer_num' in row:
      layer_num = row['layer_num']

    file_path = row['path'].replace('dbfs:','')

    gdf = gpd.read_file(file_path, layer=layer_num)
    # [2] set CRS to 4326 (WGS84)
    gdf_4326 = gdf.to_crs(epsg=4326)

    # [3] 
    gdf_wkt = gdf_4326.to_wkt

    # [3] convert 'geometry' column to wkt +
    pdf_arr.append(pd.DataFrame(gdf_4326.to_wkt()))

   # return as pandas dataframe
  return pd.concat(pdf_arr)

# COMMAND ----------

# MAGIC %md __[2] We need a schema for our return__ 
# MAGIC
# MAGIC > Will use the example from above for this; in production, you will want to be more careful defining the return schema.

# COMMAND ----------

spark.createDataFrame(pd.DataFrame(gdf_4326.to_wkt())).schema

# COMMAND ----------

layer_schema = StructType([
  StructField('TLID', LongType(), True), 
  StructField('TFIDL', LongType(), True), 
  StructField('TFIDR', LongType(), True), 
  StructField('ARIDL', StringType(), True), 
  StructField('ARIDR', StringType(), True), 
  StructField('LINEARID', StringType(), True), 
  StructField('FULLNAME', StringType(), True), 
  StructField('LFROMHN', StringType(), True), 
  StructField('LTOHN', StringType(), True), 
  StructField('RFROMHN', StringType(), True), 
  StructField('RTOHN', StringType(), True), 
  StructField('ZIPL', StringType(), True), 
  StructField('ZIPR', StringType(), True), 
  StructField('EDGE_MTFCC', StringType(), True), 
  StructField('ROAD_MTFCC', StringType(), True), 
  StructField('PARITYL', StringType(), True), 
  StructField('PARITYR', StringType(), True), 
  StructField('PLUS4L', StringType(), True), # <- altered from inferred NullType
  StructField('PLUS4R', StringType(), True), # <- altered from inferred NullType
  StructField('LFROMTYP', StringType(), True), 
  StructField('LTOTYP', StringType(), True), 
  StructField('RFROMTYP', StringType(), True), 
  StructField('RTOTYP', StringType(), True), 
  StructField('OFFSETL', StringType(), True), 
  StructField('OFFSETR', StringType(), True), 
  StructField('geometry', StringType(), True)
])

# layer_schema

# COMMAND ----------

# MAGIC %md __[3] Define Spark DataFrame with Paths__
# MAGIC
# MAGIC > We just need a list of files to process, e.g. from "address_features" directory.

# COMMAND ----------

df_path = spark.createDataFrame(dbutils.fs.ls(f"{ETL_DIR}/address_features"))
print(f"count? {df_path.count():,}")
df_path.limit(5).display() # <- limiting for ipynb only

# COMMAND ----------

# MAGIC %md __[4] Invoke the UDF__
# MAGIC
# MAGIC > Group By 'path'; also repartition by 'path' to drive parallelism.

# COMMAND ----------

# MAGIC %md __DRY-RUN:__ _LIMIT 5_

# COMMAND ----------

spark.catalog.clearCache()          # <- cache for dev, help avoid recomputes

DRY_LIMIT = 5

out_df = (
  df_path 
    .limit(DRY_LIMIT)               # <- NOTE: DRY-RUN
    .repartition(DRY_LIMIT, "path") # <-repartition                                                                            
    .groupBy("path")                # <- groupby `path`
    .applyInPandas(
      geopandas_read, schema=layer_schema
    )
    .cache()
)

print(f"count? {out_df.count():,}")
out_df.limit(5).display() # <- limiting for ipynb only

# COMMAND ----------

out_df.filter(col("plus4l").isNotNull()).count()

# COMMAND ----------

# MAGIC %md __ACTUAL:__ _Write All to Delta Lake_
# MAGIC
# MAGIC > We are saving as a managed table named 'shape_address_block'.

# COMMAND ----------

sql(f"drop table if exists {catalog_name}.{db_name}.shape_address_block")

(
  df_path 
    .repartition(num_shapefiles, "path") # <-repartition                                                                            
    .groupBy("path")                     # <- groupby `path`
    .applyInPandas(
      geopandas_read, schema=layer_schema
    )
    .write
      .mode("append")
    .saveAsTable(f"{catalog_name}.{db_name}.shape_address_block")
)

# COMMAND ----------

df_address = spark.table(f"{catalog_name}.{db_name}.shape_address_block")

print(f"count? {df_address.count():,}")
df_address.limit(1).display()

# COMMAND ----------

# MAGIC %md __NOTE: WE DID NOT ADD A SPATIAL INDEX, THAT WILL INVOLVE [1] H3 TESSELLATION and [2] Optimizing, e.g. via [ZORDER](https://docs.databricks.com/en/delta/data-skipping.html) or (newer for DBR 13.3+) [LIQUID CLUSTERING](https://docs.databricks.com/en/delta/clustering.html).__

# COMMAND ----------

# MAGIC %md ## Final Sanity Check

# COMMAND ----------

# MAGIC %sql show tables

# COMMAND ----------

# MAGIC %sql select * from shape_address_block limit 3
