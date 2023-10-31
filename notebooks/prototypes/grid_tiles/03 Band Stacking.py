# Databricks notebook source
# MAGIC %md
# MAGIC ## Install the libraries and prepare the environment

# COMMAND ----------

# MAGIC %pip install databricks-mosaic rasterio==1.3.5 --quiet gdal==3.4.3 pystac pystac_client planetary_computer tenacity rich

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
spark.conf.set("spark.sql.adaptive.enabled", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data load

# COMMAND ----------

# MAGIC %md 
# MAGIC We can easily browse the data we have downloaded in the notebook 00. The download metadata is stored as a delta table.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE odin_alaska

# COMMAND ----------

df_b02 = spark.read.table("alaska_b02_indexed")\
  .withColumn("h3", F.col("raster.index_id"))
df_b03 = spark.read.table("alaska_b03_indexed")\
  .withColumn("h3", F.col("raster.index_id"))
df_b04 = spark.read.table("alaska_b04_indexed")\
  .withColumn("h3", F.col("raster.index_id"))
df_b08 = spark.read.table("alaska_b08_indexed")\
  .withColumn("h3", F.col("raster.index_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC For the purpose of raster data analysis mosaic framework provides a distributed gdal data readers.
# MAGIC We can also retile the images on read to make sure the imagery is balanced and more parallelised.

# COMMAND ----------

df_b03.limit(20).display()

# COMMAND ----------

df_b03.groupBy("h3", "date").count().display()

# COMMAND ----------

to_plot = df_b03.limit(50).collect()

# COMMAND ----------

library.plot_raster(to_plot[37]["raster"]["raster"])

# COMMAND ----------

counts = df_b03.select("h3", "date").groupBy("h3", "date").count()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC counts h3 h3 100000000

# COMMAND ----------

df_b02_resolved = df_b02.groupBy("h3", "date")\
  .agg(mos.rst_merge(F.collect_list("raster.raster")).alias("raster"))

df_b03_resolved = df_b03.groupBy("h3", "date")\
  .agg(mos.rst_merge(F.collect_list("raster.raster")).alias("raster"))

df_b04_resolved = df_b04.groupBy("h3", "date")\
  .agg(mos.rst_merge(F.collect_list("raster.raster")).alias("raster"))

df_b08_resolved = df_b08.groupBy("h3", "date")\
  .agg(mos.rst_merge(F.collect_list("raster.raster")).alias("raster"))


# COMMAND ----------

df_b03_resolved.groupBy("h3", "date").count().display()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_b03_resolved h3 h3 1000000

# COMMAND ----------

stacked_df = df_b02_resolved\
    .repartition(200, F.rand())\
    .withColumnRenamed("raster", "b02")\
    .join(
      df_b03_resolved\
        .repartition(200, F.rand())\
        .withColumnRenamed("raster", "b03"),
      on = ["h3", "date"]
    )\
    .join(
      df_b04_resolved\
        .repartition(200, F.rand())\
        .withColumnRenamed("raster", "b04"),
      on = ["h3", "date"]
    )\
    .join(
      df_b08_resolved\
        .repartition(200, F.rand())\
        .withColumnRenamed("raster", "b08"),
      on = ["h3", "date"]
    )\
    .withColumn("raster", mos.rst_mergebands(F.array("b04", "b03", "b02", "b08"))) # b04 = red b03 = blue b02 = green b08 = nir

# COMMAND ----------

stacked_df.count()

# COMMAND ----------

stacked_df.limit(50).display()

# COMMAND ----------

ndvi_test = stacked_df.withColumn(
  "ndvi", mos.rst_ndvi("raster", F.lit(4), F.lit(1))
)

# COMMAND ----------

to_plot = ndvi_test.limit(50).collect()

# COMMAND ----------

library.plot_raster(to_plot[4]["ndvi"])

# COMMAND ----------


