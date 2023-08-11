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

catalog_df = spark.read.table("mosaic_odin_alaska_B02")\
  .repartition(300, "outputfile", F.rand())
catalog_df.display()

# COMMAND ----------

# MAGIC %fs ls /FileStore/geospatial/odin/alaska/B02

# COMMAND ----------

df_b02 = spark.read.table("alaska_b02_indexed")\
  .withColumn("h3", F.col("raster.index_id"))
df_b03 = spark.read.table("alaska_b03_indexed")\
  .withColumn("h3", F.col("raster.index_id"))
df_b04 = spark.read.table("alaska_b04_indexed")\
  .withColumn("h3", F.col("raster.index_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC For the purpose of raster data analysis mosaic framework provides a distributed gdal data readers.
# MAGIC We can also retile the images on read to make sure the imagery is balanced and more parallelised.

# COMMAND ----------

df_b02.display()

# COMMAND ----------

df_b02.groupBy("h3", "date").count().display()

# COMMAND ----------

to_plot = df_b02.limit(50).collect()

# COMMAND ----------

library.plot_raster(to_plot[42]["raster"]["raster"])

# COMMAND ----------

test = df_b02.select("h3", "date").groupBy("h3", "date").count()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC test h3 h3 100000000

# COMMAND ----------

df_b02.where("raster.raster == null").count()

# COMMAND ----------

df_b02_resolved = df_b02.groupBy("h3", "date")\
  .agg(mos.rst_merge(F.collect_list("raster.raster")).alias("raster"))

df_b03_resolved = df_b03.groupBy("h3", "date")\
  .agg(mos.rst_merge(F.collect_list("raster.raster")).alias("raster"))

df_b04_resolved = df_b04.groupBy("h3", "date")\
  .agg(mos.rst_merge(F.collect_list("raster.raster")).alias("raster"))

# COMMAND ----------

df_b02_resolved.groupBy("h3", "date").count().display()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_b02_resolved h3 h3 1000000

# COMMAND ----------

stacked_df = df_b02_resolved\
    .repartition(200, F.rand())\
    .withColumnRenamed("raster", "b02")\
    .join(
      df_b03_resolved\
        .withColumnRenamed("raster", "b03"),
      on = ["h3", "date"]
    ).join(
      df_b04_resolved\
        .withColumnRenamed("raster", "b04"),
      on = ["h3", "date"]
    )\
    .where("h3 == 603827497129213951")\
    .withColumn("raster", mos.rst_mergebands(F.array("b04", "b03", "b02"))) # b04 = red b03 = blue b02 = green

# COMMAND ----------

stacked_df.display()

# COMMAND ----------

stacked_df.createOrReplaceTempView("multiband")

# COMMAND ----------

to_plot = stacked_df.limit(50).collect()

# COMMAND ----------

from rasterio.plot import show

fig, ax = pyplot.subplots(1, figsize=(12, 12))

with MemoryFile(BytesIO(to_plot[0]["raster"])) as memfile:
  with memfile.open() as src:
    show(src.read(1), ax=ax)
    pyplot.show()

# COMMAND ----------

library.plot_raster(to_plot[0]["b01"])

# COMMAND ----------

library.plot_raster(to_plot[0]["b02"])

# COMMAND ----------

library.plot_raster(to_plot[2]["b08"])

# COMMAND ----------


