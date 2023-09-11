# Databricks notebook source
# MAGIC %md
# MAGIC ## Install the libraries and prepare the environment

# COMMAND ----------

# MAGIC %pip install databricks-mosaic rasterio==1.3.5 --quiet gdal==3.4.3 pystac pystac_client planetary_computer tenacity rich

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
spark.conf.set("spark.sql.adaptive.enabled", "false")

# COMMAND ----------

import library
import mosaic as mos
import rasterio

from io import BytesIO
from matplotlib import pyplot
from rasterio.io import MemoryFile
from pyspark.sql import functions as F

mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

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
# MAGIC USE odin_alaska;
# MAGIC SHOW TABLES;

# COMMAND ----------

catalog_df = \
  spark.read.table("alaska_b04")\
    .withColumn("souce_band", F.lit("B04"))
catalog_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC For the purpose of raster data analysis mosaic framework provides a distributed gdal data readers.
# MAGIC We can also retile the images on read to make sure the imagery is balanced and more parallelised.

# COMMAND ----------

tiles_df = catalog_df\
  .repartition(200, F.rand())\
  .withColumn("raster", mos.rst_subdivide("outputfile", F.lit(8)))\
  .withColumn("size", mos.rst_memsize("raster"))

# COMMAND ----------

# MAGIC %md
# MAGIC At this point all our imagery is held in memory, but we can easily access it and visualise it.

# COMMAND ----------

to_plot = tiles_df.limit(50).collect()

# COMMAND ----------

library.plot_raster(to_plot[12]["raster"])

# COMMAND ----------

# MAGIC %md
# MAGIC Mosaic framework provides the same tessellation principles for both vector and raster data. We can project both vector and raster data into a unified grid and from there it is very easy to combine and join raster to raster, vector to vector and raster to vector data.

# COMMAND ----------

grid_tessellate_df = tiles_df\
  .repartition(200, F.rand())\
  .withColumn("raster", mos.rst_tessellate("raster", F.lit(6)))\
  .withColumn("index_id", F.col("raster.index_id"))

# COMMAND ----------

to_plot = grid_tessellate_df.limit(50).collect()

# COMMAND ----------

library.plot_raster(to_plot[18]["raster"]["raster"])

# COMMAND ----------

def index_band(band_table, resolution):
  catalog_df = \
    spark.read.table(band_table)\
      .withColumn("souce_band", F.col("asset.name"))
  
  tiles_df = catalog_df\
    .repartition(200, F.rand())\
    .withColumn("raster", mos.rst_subdivide("outputfile", F.lit(8)))\
    .withColumn("size", mos.rst_memsize("raster"))
  
  grid_tessellate_df = tiles_df\
    .repartition(200, F.rand())\
    .withColumn("raster", mos.rst_tessellate("raster", F.lit(resolution)))\
    .withColumn("index_id", F.col("raster.index_id"))
  
  grid_tessellate_df.write.mode("overwrite").format("delta").saveAsTable(f"{band_table}_indexed")

# COMMAND ----------

tables_to_index = spark.sql("SHOW TABLES").where("tableName not like '%indexed'").select("tableName").collect()
tables_to_index = [tbl["tableName"] for tbl in tables_to_index]
tables_to_index

# COMMAND ----------

index_band("alaska_b02", 6)

# COMMAND ----------

index_band("alaska_b03", 6)

# COMMAND ----------

index_band("alaska_b04", 6)

# COMMAND ----------

grid_tessellate_df = spark.read.table("alaska_b02_indexed")
grid_tessellate_df.limit(20).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raster for arbitrary corridors.

# COMMAND ----------

# MAGIC %md
# MAGIC To illustrate how easy is to combine vector and raster data we will use a traditionally hard problem. Extraction of raster data for an arbitrary corridors.

# COMMAND ----------

line_example = "LINESTRING(-158.34445841325555 68.0176784075422,-155.55393106950555 68.0423396963395,-154.82883341325555 67.84431100260183,-159.33322794450555 67.81114172848677,-160.01438028825555 67.47684671455214,-154.43332560075555 67.56925103744871,-154.01584513200555 67.30791374746678,-160.16818888200555 67.25700024664256,-160.58566935075555 66.94924133006975,-153.73020060075555 67.0693906319206,-154.49924356950555 66.70715520513478,-160.12424356950555 66.70715520513478,-159.02561075700555 66.37476822845568,-154.56516153825555 66.49774379983036,-155.04855997575555 66.22462528148408,-158.76193888200555 66.16254082040112,-157.94895060075555 65.94851918639993,-155.64182169450555 66.0021934684043,-158.58615763200555 66.55900493948819,-155.26828653825555 67.43472555587037,-161.64035685075555 67.86087797718164,-161.66232950700555 67.44315575603868)"

line_df = spark.createDataFrame([line_example], "string")\
  .select(F.col("value").alias("wkt"))\
  .select(
    mos.grid_tessellateexplode("wkt", F.lit(6)).alias("grid")
  )\
  .select("grid.*")

# COMMAND ----------

# MAGIC %md
# MAGIC We can visualise all the cells of interest for the provided arbitrary corridor. Since we are now operating in grid space it is very easy to get all raster images that match this specification.

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC line_df index_id h3

# COMMAND ----------

cells_of_interest = grid_tessellate_df.repartition(40, F.rand()).join(line_df, on=["index_id"])

# COMMAND ----------

cells_of_interest.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Our framework provides a very easy way to provide rasterio lambda functions that we can distribute and scale up without any involvement from the end user.

# COMMAND ----------

src = rasterio.open("/dbfs/FileStore/geospatial/odin/dais23demo/1805789896tif")
avg = src.statistics(bidx = 1).mean
avg

# COMMAND ----------

def mean_band_1(dataset):
  try:
    return dataset.statistics(bidx = 1).mean
  except:
    return 0.0

with_measurement = cells_of_interest.withColumn(
  "rasterio_lambda", library.rasterio_lambda("raster.raster", lambda dataset: mean_band_1(dataset) )
)

# COMMAND ----------

with_measurement.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("mosaic_odin_gridded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raster to Timeseries projection

# COMMAND ----------

# MAGIC %md
# MAGIC With this power of expression that rasterio provides and power of distribution that mosaic provides we can easily convert rasters to numerical values with arbitrary mathematical complexity. Since all of our imagery is timestamped, our raster to number projection in effect is creating time series bound to H3 cells.

# COMMAND ----------

with_measurement = spark.read.table("mosaic_odin_gridded")

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

df_06_20 = measurements.where("date == '2021-06-20'")
df_06_03 = measurements.where("date == '2021-06-03'")

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_06_20 index_id h3 5000

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_06_03 index_id h3 5000

# COMMAND ----------


