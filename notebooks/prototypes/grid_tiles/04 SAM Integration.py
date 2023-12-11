# Databricks notebook source


# COMMAND ----------

# MAGIC %pip install rasterio==1.3.5 --quiet gdal==3.4.3 pystac pystac_client planetary_computer torch transformers

# COMMAND ----------

import library
import sam_lib
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
# MAGIC %reload_ext sam_lib

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
spark.conf.set("spark.sql.adaptive.enabled", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data load and model load

# COMMAND ----------

with_measurement = spark.read.table("mosaic_odin_gridded")

# COMMAND ----------

library.plot_raster(with_measurement.limit(50).collect()[0]["raster"])

# COMMAND ----------

import torch
from PIL import Image
import requests
from transformers import SamModel, SamProcessor
import torch

# COMMAND ----------

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model = SamModel.from_pretrained("facebook/sam-vit-huge").to(device)
processor = SamProcessor.from_pretrained("facebook/sam-vit-huge")

# COMMAND ----------

tiles = with_measurement.limit(50).collect()

# COMMAND ----------

raster = tiles[1]["raster"]
raw_image = Image.open(BytesIO(raster))

# COMMAND ----------

library.plot_raster(raster)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply SAM on one of the tiles

# COMMAND ----------

def get_masks(raw_image):
  inputs = processor(raw_image, return_tensors="pt").to(device)
  image_embeddings = model.get_image_embeddings(inputs["pixel_values"])
  inputs.pop("pixel_values", None)
  inputs.update({"image_embeddings": image_embeddings})

  with torch.no_grad():
    outputs = model(**inputs)

  masks = processor.image_processor.post_process_masks(
    outputs.pred_masks.cpu(),
    inputs["original_sizes"].cpu(),
    inputs["reshaped_input_sizes"].cpu()
  )
  return masks
  
def get_scores(raw_image):
  inputs = processor(raw_image, return_tensors="pt").to(device)
  image_embeddings = model.get_image_embeddings(inputs["pixel_values"])
  inputs.pop("pixel_values", None)
  inputs.update({"image_embeddings": image_embeddings})

  with torch.no_grad():
    outputs = model(**inputs)

  scores = outputs.iou_scores
  return scores

# COMMAND ----------

scores = get_scores(raw_image)
masks = get_masks(raw_image)

# COMMAND ----------

sam_lib.show_masks_on_image(raw_image, masks[0], scores)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Scaling model scoring with pandas UDFs

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType

# Declare the function and create the UDF
def apply_sam(rasters: pd.Series) -> pd.Series:

  device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
  model = SamModel.from_pretrained("facebook/sam-vit-huge").to(device)
  processor = SamProcessor.from_pretrained("facebook/sam-vit-huge")

  return rasters\
    .apply(raster: Image.open(BytesIO(raster)))\
    .apply(image: get_masks(image))
