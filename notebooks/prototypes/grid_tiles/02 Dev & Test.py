# Databricks notebook source
# MAGIC %md
# MAGIC ## Install the libraries and prepare the environment

# COMMAND ----------

# MAGIC %pip install rasterio==1.3.5 --quiet gdal==3.4.3 pystac pystac_client planetary_computer

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

spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data load

# COMMAND ----------

# MAGIC %md 
# MAGIC We can easily browse the data we have downloaded in the notebook 00. The download metadata is stored as a delta table.

# COMMAND ----------

catalog_df = spark.read.table("mosaic_odin_files")
catalog_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC For the purpose of raster data analysis mosaic framework provides a distributed gdal data readers.
# MAGIC We can also retile the images on read to make sure the imagery is balanced and more parallelised.

# COMMAND ----------

tiles_df = catalog_df\
  .withColumn("raster", mos.rst_subdivide("outputfile", F.lit(16)))\
  .withColumn("size", mos.rst_memsize("raster"))

# COMMAND ----------

tiles_df.limit(50).display()

# COMMAND ----------

# MAGIC %md
# MAGIC At this point all our imagery is held in memory, but we can easily access it and visualise it.

# COMMAND ----------

to_plot = tiles_df.limit(50).collect()

# COMMAND ----------

library.plot_raster(to_plot[42]["raster"])

# COMMAND ----------

# MAGIC %md
# MAGIC Mosaic framework provides the same tessellation principles for both vector and raster data. We can project both vector and raster data into a unified grid and from there it is very easy to combine and join raster to raster, vector to vector and raster to vector data.

# COMMAND ----------

grid_tessellate_df = tiles_df\
  .withColumn("raster", mos.rst_tessellate("raster", F.lit(6)))\
  .withColumn("index_id", F.col("raster.index_id"))

to_plot = grid_tessellate_df.limit(50).collect()

# COMMAND ----------

library.plot_raster(to_plot[1]["raster"]["raster"])

# COMMAND ----------

grid_tessellate_df\
  .repartition(200, "raster.index_id", F.rand())\
  .groupBy("raster.index_id", "date")\
  .agg(F.collect_list("raster.raster").alias("raster"))\
  .withColumn("raster", mos.rst_merge("raster"))\
  .write.mode("overwrite").format("delta").save("dbfs:/FileStore/geospatial/odin/dais23demo/indexed")

grid_tessellate_df = spark.read.format("delta").load("dbfs:/FileStore/geospatial/odin/dais23demo/indexed")

# COMMAND ----------

grid_tessellate_df.display()

# COMMAND ----------

library.plot_raster(to_plot[4]["raster"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raster for arbitrary corridors.

# COMMAND ----------

# MAGIC %md
# MAGIC To illustrate how easy is to combine vector and raster data we will use a traditionally hard problem. Extraction of raster data for an arbitrary corridors.

# COMMAND ----------

line_example = "LINESTRING(-122.2163001236001 47.77530703528161,-122.1503821548501 47.51996083856245,-121.8867102798501 47.62743233444236,-122.0954505142251 47.360200479212935,-121.8152991470376 47.41970286748326,-121.5131751236001 47.360200479212935,-121.7603675064126 47.23726461439514,-122.2547522720376 47.0691640536914,-121.9361487564126 47.08038730142549,-121.3813391861001 47.10282670591806,-121.2110511001626 47.31925361681828,-120.9308997329751 47.56816499155946,-120.7661048111001 47.41226874260139,-121.1616126236001 47.11404286281199,-121.7933264907876 46.885516358226546,-122.3206702407876 46.79909683431514)"

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

cells_of_interest.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Our framework provides a very easy way to provide rasterio lambda functions that we can distribute and scale up without any involvement from the end user.

# COMMAND ----------

to_plot = cells_of_interest\
  .groupBy("date")\
  .agg(F.collect_list("raster").alias("raster"))\
  .withColumn("raster", mos.rst_merge("raster"))\
  .collect()

# COMMAND ----------

library.plot_raster(to_plot[0]["raster"])

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
  "rasterio_lambda", library.rasterio_lambda("raster", lambda dataset: mean_band_1(dataset) )
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

df_12_13 = measurements.where("date == '2020-12-13'")
df_12_05 = measurements.where("date == '2020-12-05'")

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_12_13 index_id h3 5000

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_12_05 index_id h3 5000

# COMMAND ----------


