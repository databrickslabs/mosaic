# Databricks notebook source
# MAGIC %md
# MAGIC ## Install and enable mosaic. 
# MAGIC We will leverage both vector and raster data so we will also enable GDAL support in mosaic.

# COMMAND ----------

import mosaic as mos
import pyspark.sql.functions as F
mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

# COMMAND ----------

spark.conf.set("spark.databricks.optimizer.adaptive.enabled", "false")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE flood_risk

# COMMAND ----------

# MAGIC %md
# MAGIC Load the risk results and visualise. </br>
# MAGIC Resample to resolution 7 due to data volume.

# COMMAND ----------

scored = spark.read.table("scored").select("cell_id", "predictions")\
  .withColumn("cell_id", F.expr("h3_toparent(cell_id, 7)"))\
  .groupBy("cell_id")\
  .agg(F.avg("predictions"))

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC scored cell_id h3 400000

# COMMAND ----------

results_full = spark.read.table("risk_scores_full").select("cell_id", "predictions")

# COMMAND ----------

to_show_full = results_full.groupBy(
  F.expr("h3_toparent(cell_id, 6)").alias("cell_id")
).agg(
  F.avg("predictions").alias("target")
)

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC to_show_full cell_id h3 400000

# COMMAND ----------

roads = spark.read.table("roads")\
  .select(
    F.lit("road").alias("asset_type"),
    F.col("FULLNAME").alias("asset_name"),
    mos.st_astext(F.col("grid.wkb")).alias("geom"),
    F.col("grid.index_id").alias("cell_id")
  )

bridges = spark.read.table("bridges")\
  .select(
    F.lit("bridge").alias("asset_type"),
    F.col("COMMON_NAME").alias("asset_name"),
    F.col("geom_0").alias("geom"),
    F.col("cell_id")
  )

infrastructure = roads.union(bridges)

# COMMAND ----------

infrastructure_risk = spark.read.table("scored")\
  .select("cell_id", "predictions")\
  .join(infrastructure, on=["cell_id"])

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC infrastructure_risk geom geometry 50000

# COMMAND ----------

  high_risk = infrastructure_risk.where(F.col("predictions") > F.lit(4.5))

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC high_risk geom geometry 50000

# COMMAND ----------


