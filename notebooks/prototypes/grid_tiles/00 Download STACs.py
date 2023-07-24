# Databricks notebook source
# MAGIC %md
# MAGIC ## Install the libraries and prepare the environment

# COMMAND ----------

# MAGIC %md
# MAGIC For this demo we will require a few spatial libraries that can be easily installed via pip install. We will be using gdal, rasterio, pystac and databricks-mosaic for data download and data manipulation. We will use planetary computer as the source of the raster data for the analysis.

# COMMAND ----------

# MAGIC %pip install databricks-mosaic rasterio==1.3.5 --quiet gdal==3.4.3 pystac pystac_client planetary_computer

# COMMAND ----------

import library
import pystac_client
import planetary_computer
import mosaic as mos

from pyspark.sql import functions as F

mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC We have selected an area near Seatle for this demo, this is just an illustrative choice, the code will work with an area anywhere on the surface of the planet. Our solution provides an easy way to tesselate the area into indexed pieces. This tessellation will allow us to parallelise the download of the data.

# COMMAND ----------

cells = library.generate_cells((-123, 47, -122, 48), 8, spark, mos)
cells.display()

# COMMAND ----------

to_display = cells.select("grid.wkb")

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC to_display wkb geometry 200000

# COMMAND ----------

# MAGIC %md
# MAGIC It is fairly easy to interface with the pysta_client and a remote raster data catalogs. We can browse resource collections and individual assets.

# COMMAND ----------

catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace,
)

# COMMAND ----------

collections = list(catalog.get_collections())
collections

# COMMAND ----------

time_range = "2020-12-01/2020-12-31"
bbox = [-123, 47, -122, 48]

search = catalog.search(collections=["landsat-c2-l2"], bbox=bbox, datetime=time_range)
items = search.item_collection()
items

# COMMAND ----------

[json.dumps(item.to_dict()) for item in items]

# COMMAND ----------

cell_jsons = cells.select(
  F.hash("geom").alias("area_id"),
  F.col("grid.index_id").alias("h3"),
  mos.st_asgeojson("grid.wkb").alias("geojson")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Stac catalogs support easy download for area of interest provided as geojsons. With this in mind we will convert all our H3 cells of interest into geojsons and prepare stac requests.

# COMMAND ----------

cell_jsons.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Our framework allows for easy preparation of stac requests with only one line of code. This data is delta ready as this point and can easily be stored for lineage purposes.

# COMMAND ----------

eod_items = library.get_assets_for_cells(cell_jsons.limit(200))
eod_items.display()

# COMMAND ----------

# MAGIC %md
# MAGIC From this point we can easily extract the download links for items of interest.

# COMMAND ----------

to_download = library.get_unique_hrefs(eod_items)
to_download.display()

# COMMAND ----------

# MAGIC %fs mkdirs /FileStore/geospatial/odin/dais23demo

# COMMAND ----------

catalof_df = to_download\
  .withColumn("outputfile", library.download_asset("href", F.lit("/dbfs/FileStore/geospatial/odin/dais23demo"), F.concat(F.hash(F.rand()), F.lit("tif"))))

# COMMAND ----------

catalof_df.write.format("delta").saveAsTable("mosaic_odin_files")

# COMMAND ----------

# MAGIC %md
# MAGIC We have now dowloaded all the tile os interest and we can browse them from our delta table.

# COMMAND ----------

catalof_df = spark.read.table("mosaic_odin_files")

# COMMAND ----------

catalof_df.display()

# COMMAND ----------

import rasterio
from matplotlib import pyplot
from rasterio.plot import show

fig, ax = pyplot.subplots(1, figsize=(12, 12))
raster = rasterio.open("""/dbfs/FileStore/geospatial/odin/dais23demo/1219604474tif""")
show(raster, ax=ax, cmap='Greens')
pyplot.show()

# COMMAND ----------


