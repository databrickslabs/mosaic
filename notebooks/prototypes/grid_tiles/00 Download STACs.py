# Databricks notebook source
# MAGIC %md
# MAGIC ## Install the libraries and prepare the environment

# COMMAND ----------

# MAGIC %md
# MAGIC For this demo we will require a few spatial libraries that can be easily installed via pip install. We will be using gdal, rasterio, pystac and databricks-mosaic for data download and data manipulation. We will use planetary computer as the source of the raster data for the analysis.

# COMMAND ----------

# MAGIC %pip install databricks-mosaic rasterio==1.3.5 --quiet gdal==3.4.3 pystac pystac_client planetary_computer tenacity rich

# COMMAND ----------

import library
import pystac_client
import planetary_computer
import mosaic as mos

from pyspark.sql import functions as F

mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

# COMMAND ----------

# MAGIC %reload_ext autoreload
# MAGIC %autoreload 2
# MAGIC %reload_ext library

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC We will download census data from TIGER feed for this demo. The data can be downloaded as a zip to dbfs (or managed volumes).

# COMMAND ----------

dbutils.fs.rm("/FileStore/geospatial/odin/census/", True)
dbutils.fs.mkdirs("/FileStore/geospatial/odin/census/")

# COMMAND ----------

import urllib.request
urllib.request.urlretrieve(
  "https://www2.census.gov/geo/tiger/TIGER2021/COUNTY/tl_2021_us_county.zip",
  "/dbfs/FileStore/geospatial/odin/census/data.zip"
)

# COMMAND ----------

# MAGIC %sh ls -al /dbfs/FileStore/geospatial/odin/census/

# COMMAND ----------

# MAGIC %md
# MAGIC Mosaic has specialised readers for shape files and other GDAL supported formats. We dont need to unzip the data zip file. Just need to pass "vsizip" option to the reader.

# COMMAND ----------

census_df = mos.read().format("multi_read_ogr")\
  .option("vsizip", "true")\
  .option("chunkSize", "50")\
  .load("dbfs:/FileStore/geospatial/odin/census/data.zip")\
  .cache() # We will cache the loaded data to avoid schema inference being done repeatedly for each query

# COMMAND ----------

# MAGIC %md
# MAGIC For this exmaple we will focus on Alaska counties. Alska state code is 02 so we will apply a filter to our ingested data.

# COMMAND ----------

census_df.where("STATEFP == 2").display()

# COMMAND ----------

to_display = census_df\
  .where("STATEFP == 2")\
  .withColumn(
    "geom_0",
    mos.st_updatesrid("geom_0", "geom_0_srid", F.lit(4326))
  )\
  .select("geom_0")

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC to_display geom_0 geometry 50

# COMMAND ----------

cells = census_df\
  .where("STATEFP == 2")\
  .withColumn(
    "geom_0",
    mos.st_updatesrid("geom_0", "geom_0_srid", F.lit(4326))
  )\
  .withColumn("geom_0_srid", F.lit(4326))\
  .withColumn(
    "grid",
    mos.grid_tessellateexplode("geom_0", F.lit(3))
  )

# COMMAND ----------

cells.display()

# COMMAND ----------

to_display = cells.select(mos.st_simplify("grid.wkb", F.lit(0.1)).alias("wkb"))

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC to_display wkb geometry 100000

# COMMAND ----------

# MAGIC %md
# MAGIC It is fairly easy to interface with the pysta_client and a remote raster data catalogs. We can browse resource collections and individual assets.

# COMMAND ----------

time_range = "2021-06-01/2021-06-30"

# COMMAND ----------

cell_jsons = cells\
  .withColumn("area_id", F.hash("geom_0"))\
  .withColumn("h3", F.col("grid.index_id"))\
  .groupBy("h3")\
  .agg(
    mos.st_union_agg("grid.wkb").alias("geom_1")
  )\
  .withColumn("geojson", mos.st_asgeojson(mos.grid_boundaryaswkb("h3")))\
  .drop("count", "geom_1")

# COMMAND ----------

# MAGIC %md
# MAGIC Stac catalogs support easy download for area of interest provided as geojsons. With this in mind we will convert all our H3 cells of interest into geojsons and prepare stac requests.

# COMMAND ----------

cell_jsons.display()

# COMMAND ----------

cell_jsons.count()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC cell_jsons h3 h3

# COMMAND ----------

# MAGIC %md
# MAGIC Our framework allows for easy preparation of stac requests with only one line of code. This data is delta ready as this point and can easily be stored for lineage purposes.

# COMMAND ----------

eod_items = library.get_assets_for_cells(cell_jsons.repartition(200), time_range ,"sentinel-2-l2a" ).cache()
eod_items.display()

# COMMAND ----------

# MAGIC %md
# MAGIC From this point we can easily extract the download links for items of interest.

# COMMAND ----------

dbutils.fs.rm("/FileStore/geospatial/odin/alaska/", True)
dbutils.fs.mkdirs("/FileStore/geospatial/odin/alaska/")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS odin_alaska CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS odin_alaska;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE odin_alaska;

# COMMAND ----------

def download_band(eod_items, band_name):
  to_download = eod_items\
    .withColumn("timestamp", F.col("item_properties.datetime"))\
    .groupBy("item_id", "timestamp")\
    .agg(
      *[F.first(cn).alias(cn) for cn in eod_items.columns if cn not in ["item_id"]]
    )\
    .withColumn("date", F.to_date("timestamp"))\
    .withColumn("href", F.col("asset.href"))\
    .where(
      f"asset.name == '{band_name}'"
    )
  
  spark.sql(f"DROP TABLE IF EXISTS alaska_{band_name}")
  dbutils.fs.rm(f"/FileStore/geospatial/odin/alaska/{band_name}", True)
  dbutils.fs.mkdirs(f"/FileStore/geospatial/odin/alaska/{band_name}")

  catalof_df = to_download\
    .withColumn(
      "outputfile", 
      library.download_asset("href", F.lit(f"/dbfs/FileStore/geospatial/odin/alaska/{band_name}"),
      F.concat(F.hash(F.rand()), F.lit(".tif")))
    )
  
  catalof_df.write\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .format("delta")\
    .saveAsTable(f"alaska_{band_name}")


# COMMAND ----------

import rich.table

region = census_df.where("STATEFP == 2").select(mos.st_asgeojson("geom_0").alias("geojson")).limit(1).collect()[0]["geojson"]

catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace,
)

search = catalog.search(
    collections=["sentinel-2-l2a"],
    intersects=region, 
    datetime=time_range
)

items = search.item_collection()

table = rich.table.Table("Asset Key", "Description")
for asset_key, asset in items[0].assets.items():
    table.add_row(asset_key, asset.title)

table

# COMMAND ----------

bands = []
for asset_key, asset in items[0].assets.items():
  bands.append(asset_key)

bands = [b for b in bands if b not in ["visual", "preview", "safe-manifest", "tilejson", "rendered_preview", "granule-metadata", "inspire-metadata", "product-metadata", "datastrip-metadata"]]
bands

# COMMAND ----------

for band in bands:
  download_band(eod_items, band)

# COMMAND ----------

# MAGIC %fs ls /FileStore/geospatial/odin/alaska/B08

# COMMAND ----------

import rasterio
from matplotlib import pyplot
from rasterio.plot import show

fig, ax = pyplot.subplots(1, figsize=(12, 12))
raster = rasterio.open("""/dbfs/FileStore/geospatial/odin/alaska/B08/2764922.tif""")
show(raster, ax=ax, cmap='Greens')
pyplot.show()

# COMMAND ----------


