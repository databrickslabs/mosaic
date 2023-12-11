# Databricks notebook source
# MAGIC %md
# MAGIC ## Install the libraries and prepare the environment

# COMMAND ----------

# MAGIC %pip install databricks-mosaic rasterio==1.3.5 --quiet gdal==3.4.3 pystac pystac_client planetary_computer tenacity rich pandas==1.5.3

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
spark.conf.set("spark.sql.adaptive.enabled", "false")
# spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", "100")
spark.conf.set("spark.databricks.labs.mosaic.index.system", "BNG")
spark.conf.set("spark.databricks.labs.mosaic.geometry.api", "JTS")

# COMMAND ----------

import mosaic as mos
from pyspark.sql import functions as F

mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

# COMMAND ----------

import library
import rasterio

from io import BytesIO
from matplotlib import pyplot
from rasterio.io import MemoryFile

# COMMAND ----------

# MAGIC %reload_ext autoreload
# MAGIC %autoreload 2
# MAGIC %reload_ext library

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data load

# COMMAND ----------

# MAGIC %md 
# MAGIC We can easily browse the data we have downloaded in the notebook 00. The download metadata is stored as a delta table.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE odin_uk;
# MAGIC SHOW TABLES;

# COMMAND ----------

catalog_df = \
  spark.read.table("uk_b02")\
    .withColumn("souce_band", F.lit("B02"))\
    .repartition(200)\
    .cache()
catalog_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC For the purpose of raster data analysis mosaic framework provides a distributed gdal data readers.
# MAGIC We can also retile the images on read to make sure the imagery is balanced and more parallelised.

# COMMAND ----------

# rst_tile -> rst_load
tiles_df = catalog_df\
  .repartition(200)\
  .withColumn("tile", mos.rst_tile("outputfile", F.lit(32)))\
  .withColumn("tile", mos.rst_subdivide("tile", F.lit(8)))

# COMMAND ----------

tiles_df = (
  spark.read
    .format("gdal")
    .option("raster_storage", "in-memory")
    .load("dbfs:/FileStore/geospatial/odin/uk/B08")
    .withColumn("tile", mos.rst_subdivide("tile", F.lit(32)))
    .withColumn("size", mos.rst_memsize("tile"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC At this point all our imagery is held in memory, but we can easily access it and visualise it.

# COMMAND ----------

tiles_df.count()

# COMMAND ----------

to_plot = tiles_df.limit(50).collect()

# COMMAND ----------

library.plot_raster(to_plot[4]["tile"]["raster"])

# COMMAND ----------

# MAGIC %md
# MAGIC Mosaic framework provides the same tessellation principles for both vector and raster data. We can project both vector and raster data into a unified grid and from there it is very easy to combine and join raster to raster, vector to vector and raster to vector data.

# COMMAND ----------

grid_tessellate_df = tiles_df\
  .repartition(200)\
  .withColumn("tile", mos.rst_tessellate("tile", F.lit("1km")))\
  .withColumn("index_id", F.col("tile.index_id"))

to_plot = grid_tessellate_df.limit(50).collect()

# COMMAND ----------

library.plot_raster(to_plot[23]["tile"]["raster"])

# COMMAND ----------

grid_tessellate_df.display()

# COMMAND ----------

def index_band(band_table, resolution):
  catalog_df = \
    spark.read.table(band_table)\
      .withColumn("souce_band", F.col("asset.name"))
  
  tiles_df = catalog_df\
    .repartition(200)\
    .withColumn("tile", mos.rst_tile("outputfile", F.lit(100)))\
    .where(mos.rst_tryopen("tile"))\
    .withColumn("tile", mos.rst_subdivide("tile", F.lit(8)))\
    .withColumn("size", mos.rst_memsize("tile"))
  
  grid_tessellate_df = tiles_df\
    .repartition(200)\
    .withColumn("tile", mos.rst_tessellate("tile", F.lit(resolution)))\
    .withColumn("index_id", F.col("tile.index_id"))\
    .repartition(200)
  
  grid_tessellate_df\
    .write.mode("overwrite")\
    .option("overwriteSchema", "true")\
    .format("delta")\
    .saveAsTable(f"{band_table}_indexed")

# COMMAND ----------

tables_to_index = spark.sql("SHOW TABLES")\
  .where("tableName not like '%indexed'")\
  .where("tableName not like '%gridded'")\
  .where("tableName not like '%tmp%'")\
  .where("tableName not like '%tiles%'")\
  .select("tableName").collect()
tables_to_index = [tbl["tableName"] for tbl in tables_to_index]
tables_to_index

# COMMAND ----------

for tbl in tables_to_index:
  index_band(tbl, "1km")

# COMMAND ----------

grid_tessellate_df = spark.read.table("uk_b02_indexed")
grid_tessellate_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raster for arbitrary corridors.

# COMMAND ----------

# MAGIC %md
# MAGIC To illustrate how easy is to combine vector and raster data we will use a traditionally hard problem. Extraction of raster data for an arbitrary corridors.

# COMMAND ----------

# MAGIC %fs ls /FileStore/geospatial/odin/os_census/

# COMMAND ----------

greenspace_df = mos.read().format("multi_read_ogr")\
  .option("vsizip", "false")\
  .option("chunkSize", "500")\
  .load("dbfs:/FileStore/geospatial/odin/os_census/OS Open Greenspace (ESRI Shape File) GB/data/GB_GreenspaceSite.shp")\
  .cache()

# COMMAND ----------

greenspace_df.display()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC greenspace_df geom_0 geometry(bng) 100

# COMMAND ----------

aoi_df = greenspace_df\
  .select(F.col("geom_0").alias("wkt"))\
  .withColumn("feature_id", F.hash("wkt"))\
  .select(
    mos.grid_tessellateexplode("wkt", F.lit("1km")).alias("grid"),
    "feature_id"
  )\
  .select("grid.*", "feature_id")\
  .withColumn("wkt", mos.st_astext("wkb"))

# COMMAND ----------

aoi_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC We can visualise all the cells of interest for the provided arbitrary corridor. Since we are now operating in grid space it is very easy to get all raster images that match this specification.

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC aoi_df wkt geometry(bng)

# COMMAND ----------

cells_of_interest = grid_tessellate_df.repartition(200).join(aoi_df, on=["index_id"])

# COMMAND ----------

cells_of_interest.display()

# COMMAND ----------

result = cells_of_interest\
  .withColumn("geojson", mos.st_buffer(mos.st_setsrid(mos.as_json(mos.st_asgeojson("wkb")), F.lit(27700)), F.lit(10)))\
  .select("date", "index_id", "tile", "geojson", "feature_id")\
  .withColumn("tile", mos.rst_clip("tile", "geojson"))\
  .withColumn("parent_id", F.substring("index_id", 0, 3))\
  .groupBy("feature_id", "parent_id").agg(
    mos.rst_merge_agg("tile").alias("tile")
  )\
  .groupBy("feature_id").agg(
    mos.rst_merge_agg("tile").alias("tile")
  )
  
result.display()

# COMMAND ----------

result.write.mode("overwrite").saveAsTable("uk_green_spaces_tiles")

# COMMAND ----------

spark.read.table("uk_green_spaces_tiles").count()

# COMMAND ----------

to_plot = spark.read.table("uk_green_spaces_tiles").orderBy(F.length("tile.raster").desc()).limit(100).collect()

# COMMAND ----------

library.plot_raster(to_plot[1]["tile"]["raster"])

# COMMAND ----------

library.plot_raster(to_plot[2]["tile"]["raster"])

# COMMAND ----------

library.plot_raster(to_plot[23]["tile"]["raster"])

# COMMAND ----------

# MAGIC %md
# MAGIC Our framework provides a very easy way to provide rasterio lambda functions that we can distribute and scale up without any involvement from the end user.

# COMMAND ----------

def mean_band_1(dataset):
  try:
    return dataset.statistics(bidx = 1).mean
  except:
    return 0.0

with_measurement = cells_of_interest.withColumn(
  "rasterio_lambda", library.rasterio_lambda("tile.raster", lambda dataset: mean_band_1(dataset) )
)

# COMMAND ----------

with_measurement.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("mosaic_odin_uk_gridded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raster to Timeseries projection

# COMMAND ----------

# MAGIC %md
# MAGIC With this power of expression that rasterio provides and power of distribution that mosaic provides we can easily convert rasters to numerical values with arbitrary mathematical complexity. Since all of our imagery is timestamped, our raster to number projection in effect is creating time series bound to H3 cells.

# COMMAND ----------

with_measurement = spark.read.table("mosaic_odin_uk_gridded")

# COMMAND ----------

with_measurement.where("rasterio_lambda > 0").display()

# COMMAND ----------

measurements = with_measurement\
  .select(
    "index_id",
    "date",
    "rasterio_lambda",
    "wkb"
  )\
  .where("rasterio_lambda > 0")\
  .groupBy("index_id", "date")\
  .agg(
    F.avg("rasterio_lambda").alias("measure"),
    F.first("wkb").alias("wkb")
  )

# COMMAND ----------

# MAGIC %md
# MAGIC At this point our data is effectively became timeseries data and can be modeled as virtual IOT  devices that are fixed in spatial context.

# COMMAND ----------

measurements.display()

# COMMAND ----------

# MAGIC %md
# MAGIC We can easily visualise data for individual dates in a spatial contex by leveraging our H3 locations.

# COMMAND ----------

df_06_03 = measurements.where("date == '2021-06-03'").drop("tile")

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_06_03 index_id bng 5000

# COMMAND ----------

grid_tessellate_df = spark.read.table("uk_b02_indexed")
grid_tessellate_df.display()

# COMMAND ----------

kring_images = grid_tessellate_df\
  .where(mos.rst_tryopen("tile"))\
  .withColumn("kring", mos.grid_cellkringexplode("index_id", F.lit(1)))\
  .groupBy("kring")\
  .agg(mos.rst_merge_agg("tile").alias("tile"))

# COMMAND ----------

to_plot = kring_images.limit(50).collect()

# COMMAND ----------

library.plot_raster(to_plot[2][1]["raster"])

# COMMAND ----------

# for peatlands is 984 instead of 960
expanded_imgs = kring_images\
  .withColumn("bbox", F.expr("rst_boundingbox(tile)"))\
  .withColumn("cent", mos.st_centroid("bbox"))\
  .withColumn("cent_cell", mos.grid_pointascellid("cent", F.lit("1km")))\
  .withColumn("clip_region", mos.grid_boundaryaswkb("cent_cell"))\
  .withColumn("clip_region", mos.st_buffer(mos.st_setsrid(mos.as_json(mos.st_asgeojson("clip_region")), F.lit(27700)), F.lit(16)))\
  .where(mos.st_area("clip_region") > 0)\
  .withColumn("tile", mos.rst_clip("tile", "clip_region"))

# COMMAND ----------

to_plot = expanded_imgs.limit(50).collect()

# COMMAND ----------

library.plot_raster(to_plot[2][1]["raster"])

# COMMAND ----------

to_plot = expanded_imgs\
  .where(F.array_contains(mos.grid_cellkring("kring", F.lit(1)), F.lit("TQ3757")))\
  .select("bbox", "clip_region", "cent_cell")
  

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC to_plot bbox geometry(bng)

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC to_plot clip_region geometry(bng)

# COMMAND ----------

rolling_tiles = expanded_imgs\
  .withColumn("tile", mos.rst_to_overlapping_tiles("tile", F.lit(30), F.lit(30), F.lit(50)))\
  .withColumn("bbox", mos.rst_boundingbox("tile"))

# COMMAND ----------

to_plot = rolling_tiles.limit(50).collect()

# COMMAND ----------

library.plot_raster(to_plot[1]["tile"]["raster"])

# COMMAND ----------

to_plot = rolling_tiles.select("bbox")

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC to_plot bbox geometry(bng)

# COMMAND ----------


