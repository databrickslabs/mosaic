# Databricks notebook source
# MAGIC %md
# MAGIC # Expore the created datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Mosaic

# COMMAND ----------

# MAGIC %pip install databricks-mosaic

# COMMAND ----------

import pyspark.sql.functions as f
import mosaic as mos
mos.enable_mosaic(spark, dbutils)

# COMMAND ----------

buildings = spark.read.table("open_street_maps.buildings")
hospital_buildings = spark.read.table("open_street_maps.hospital_buildings")
residential_buildings = spark.read.table("open_street_maps.residential_buildings")
train_station_buildings = spark.read.table("open_street_maps.train_station_buildings")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Residential buildings heatmap

# COMMAND ----------

residential_buildings_counts = (residential_buildings
   .groupBy("centroid_index_res_6")
   .count()
)

# COMMAND ----------

# MAGIC %python
# MAGIC %%mosaic_kepler
# MAGIC residential_buildings_counts "centroid_index_res_6" "h3" 100000

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train stations heatmap

# COMMAND ----------

train_station_buildings_counts = (train_station_buildings
   .groupBy("centroid_index_res_6")
   .count()
)

# COMMAND ----------

# MAGIC %python
# MAGIC %%mosaic_kepler
# MAGIC train_station_buildings_counts "centroid_index_res_6" "h3" 100000

# COMMAND ----------

# MAGIC %md
# MAGIC ## Most building-dense area

# COMMAND ----------

dense_residential_buildings = (residential_buildings
   .groupBy("centroid_index_res_6")
   .count()
   .sort(f.col("count").desc())
   .limit(1)
   .join(residential_buildings, "centroid_index_res_6")
)

# COMMAND ----------

# MAGIC %python
# MAGIC %%mosaic_kepler
# MAGIC dense_residential_buildings "polygon" "geometry" 100000

# COMMAND ----------


