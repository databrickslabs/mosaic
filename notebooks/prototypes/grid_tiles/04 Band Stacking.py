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

# MAGIC %fs ls /FileStore/geospatial/odin/

# COMMAND ----------

df_b01 = spark.read.format("delta").load("dbfs:/FileStore/geospatial/odin/alaska_indexed_B01/")\
  .withColumn("h3", F.col("raster.index_id"))
df_b02 = spark.read.format("delta").load("dbfs:/FileStore/geospatial/odin/alaska_indexed_B02/")\
  .withColumn("h3", F.col("raster.index_id"))
df_b08 = spark.read.format("delta").load("dbfs:/FileStore/geospatial/odin/alaska_indexed_B08/")\
  .withColumn("h3", F.col("raster.index_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC For the purpose of raster data analysis mosaic framework provides a distributed gdal data readers.
# MAGIC We can also retile the images on read to make sure the imagery is balanced and more parallelised.

# COMMAND ----------

df_b01.display()

# COMMAND ----------

df_b01.groupBy("h3", "date").count().display()

# COMMAND ----------

to_plot = df_b01.limit(50).collect()

# COMMAND ----------

library.plot_raster(to_plot[42]["raster"]["raster"])

# COMMAND ----------

test = df_b01.select("h3", "date").groupBy("h3", "date").count()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC test h3 h3 100000000

# COMMAND ----------

df_b02.where("raster.raster == null").count()

# COMMAND ----------

df_b01_resolved = df_b01.groupBy("h3", "date")\
  .agg(mos.rst_merge(F.collect_list("raster.raster")).alias("raster"))

df_b02_resolved = df_b02.groupBy("h3", "date")\
  .agg(mos.rst_merge(F.collect_list("raster.raster")).alias("raster"))

df_b08_resolved = df_b08.groupBy("h3", "date")\
  .agg(mos.rst_merge(F.collect_list("raster.raster")).alias("raster"))

# COMMAND ----------

df_b02_resolved.groupBy("h3", "date").count().display()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_b01_resolved h3 h3 1000000

# COMMAND ----------

stacked_df = df_b01_resolved\
    .repartition(200, F.rand())\
    .withColumnRenamed("raster", "b01")\
    .join(
      df_b02_resolved\
        .withColumnRenamed("raster", "b02"),
      on = ["h3", "date"]
    ).join(
      df_b08_resolved\
        .withColumnRenamed("raster", "b08"),
      on = ["h3", "date"]
    )\
    .where("h3 == 603827497129213951")\
    .withColumn("raster", mos.rst_mergebands(F.array("b01", "b02", "b08")))

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

# MAGIC %scala
# MAGIC
# MAGIC val example = spark.table("multiband").collect()
# MAGIC
# MAGIC val b1 = example(0)(2)
# MAGIC val b2 = example(0)(3)
# MAGIC val b8 = example(0)(4)
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.labs.mosaic.core.raster.MosaicRaster
# MAGIC import com.databricks.labs.mosaic.core.raster.operator.gdal._
# MAGIC import com.databricks.labs.mosaic.utils.PathUtils
# MAGIC import com.databricks.labs.mosaic.core.raster.api.RasterAPI.GDAL
# MAGIC import org.apache.spark.sql.types._
# MAGIC
# MAGIC val r1 = GDAL.readRaster(b1, BinaryType)
# MAGIC val r2 = GDAL.readRaster(b2, BinaryType)
# MAGIC val r8 = GDAL.readRaster(b8, BinaryType)
# MAGIC
# MAGIC val vrt = GDALBuildVRT.executeVRT(
# MAGIC   "/tmp_vrt.vrt",
# MAGIC   false,
# MAGIC   Seq(r1, r2, r8),
# MAGIC   "gdalbuildvrt -separate -resolution highest"
# MAGIC )
# MAGIC
# MAGIC val tif_res = GDALTranslate.executeTranslate(
# MAGIC   "/tmp_tif_4.tif",
# MAGIC   false,
# MAGIC   vrt,
# MAGIC   "gdal_translate -r lanczos -of GTIFF -co COMPRESS=PACKBITS"
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sh ls /

# COMMAND ----------

import rasterio
from matplotlib import pyplot
from rasterio.plot import show

fig, ax = pyplot.subplots(1, figsize=(12, 12))

src = rasterio.open("/tmp_tif_4.tif")
show(src.read(1), ax=ax)
pyplot.show()

# COMMAND ----------

src.count

# COMMAND ----------

import rasterio
from matplotlib import pyplot
from rasterio.plot import show

src = rasterio.open("/tmp_tif.tif")
show(src)

# COMMAND ----------

# MAGIC %scala
# MAGIC import  org.apache.spark.unsafe.types.UTF8String
# MAGIC
# MAGIC val path = UTF8String.fromString("/tmp_tif_2.tif")
# MAGIC val rst1 = GDAL.readRaster(path, StringType)
# MAGIC
# MAGIC rst1.getGeoTransform

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC org.gdal.gdal.gdal.GetConfigOption("CPL_TMPDIR")

# COMMAND ----------

# MAGIC %sh ls -al /local_disk0/tmp/mosaic_775da700-50ff-495b-8916-e9f44a7eca844901378916596604352

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val path = UTF8String.fromString("/local_disk0/tmp/mosaic_775da700-50ff-495b-8916-e9f44a7eca844901378916596604352/tmp_tif.tif.ovr")
# MAGIC val rst2 = GDAL.readRaster(path, StringType)
# MAGIC
# MAGIC rst2.getRaster.GetRasterBand(1).GetOverviewCount()
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC rst1.getPath

# COMMAND ----------

example2 = example.withColumn(
  "stacked", mos.rst_merge(F.array("b01", "b02", "b08"))
)

# COMMAND ----------

to_plot = example2.limit(50).collect()

# COMMAND ----------

library.plot_raster(to_plot[1]["stacked"])

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
  .write.mode("overwrite").format("delta").save("dbfs:/FileStore/geospatial/odin/dais23demo_indexed")

grid_tessellate_df = spark.read.format("delta").load("dbfs:/FileStore/geospatial/odin/dais23demo_indexed")

# COMMAND ----------

grid_tessellate_df.display()

# COMMAND ----------

to_plot = grid_tessellate_df.limit(50).collect()

# COMMAND ----------

library.plot_raster(to_plot[45]["raster"])

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

with_measurement = spark.read.table("mosaic_odin_gridded")

# COMMAND ----------

library.plot_raster(with_measurement.limit(50).collect()[0]["raster"])

# COMMAND ----------

# MAGIC %pip install torch transformers

# COMMAND ----------

import torch
from PIL import Image
import requests
from transformers import SamModel, SamProcessor

# COMMAND ----------

device = "cuda" if torch.cuda.is_available() else "cpu"
model = SamModel.from_pretrained("facebook/sam-vit-huge").to(device)
processor = SamProcessor.from_pretrained("facebook/sam-vit-huge")

# COMMAND ----------

img_url = "https://huggingface.co/ybelkada/segment-anything/resolve/main/assets/car.png"
raw_image = Image.open(requests.get(img_url, stream=True).raw).convert("RGB")
input_points = [[[450, 600]]]  # 2D location of a window in the image

# COMMAND ----------

inputs = processor(raw_image, input_points=input_points, return_tensors="pt").to(device)
outputs = model(**inputs)

# COMMAND ----------

masks = processor.image_processor.post_process_masks(
    outputs.pred_masks.cpu(), inputs["original_sizes"].cpu(), inputs["reshaped_input_sizes"].cpu()
)
scores = outputs.iou_scores

# COMMAND ----------

scores

# COMMAND ----------


