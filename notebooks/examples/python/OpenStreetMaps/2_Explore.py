# Databricks notebook source
import pyspark.sql.functions as f

polygons = spark.read.table("open_street_maps.relation_lines")

polygons.filter(f.col("type") == "multipolygon").filter(f.col("building").isNotNull()).drop("line").display()

# COMMAND ----------

buildings = spark.read.table("open_street_maps.buildings")

buildings.groupBy("building").count().orderBy(f.col("count").desc()).display()

# COMMAND ----------

# MAGIC %pip install https://github.com/databrickslabs/mosaic/releases/download/v0.1.1/databricks_mosaic-0.1.1-py3-none-any.whl

# COMMAND ----------

import mosaic as mos

mos.enable_mosaic(spark, dbutils)

# COMMAND ----------

buildings = spark.read.table("open_street_maps.buildings")

# COMMAND ----------

# MAGIC %python
# MAGIC %%mosaic_kepler
# MAGIC "open_street_maps.hospital_buildings" "polygon" "geometry" 10000

# COMMAND ----------


