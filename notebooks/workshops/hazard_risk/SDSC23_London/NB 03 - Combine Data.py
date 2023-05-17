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

# MAGIC %md
# MAGIC Set the flood_risk database.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE flood_risk;

# COMMAND ----------

roads_df = spark.read.table("roads")\
  .withColumn("geom", F.col("grid.wkb"))\
  .withColumn("cell_id", F.col("grid.index_id"))
precipitation_nyc_df = spark.read.table("weather")
bridges_df = spark.read.table("bridges")
nhda_area_df = spark.read.table("nhda_area")\
  .withColumn("geom", F.col("cell_id.wkb"))\
  .withColumn("cell_id", F.col("cell_id.index_id"))
flood_risk_model = (
  spark.read.table("flood_risk_model")
    .select("monthseq", "geom", "cell_id", "flodfreqcl")
    .where(F.col("flodfreqcl").isNotNull and F.col("flodfreqcl") != "None")
    .withColumn(
      "target", 
      F.when(F.col("flodfreqcl") == "Very frequent", 5)\
        .when(F.col("flodfreqcl") == "Frequent", 4)\
        .when(F.col("flodfreqcl") == "Occasional", 3)\
        .when(F.col("flodfreqcl") == "Rare", 2)\
        .when(F.col("flodfreqcl") == "Very rare", 1)\
        .otherwise(0)
    )
    .where("target != 0")
    .where("monthseq == 1")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Combine available data layers using the grid ID as join key.

# COMMAND ----------

train_df = roads_df\
  .withColumn("join_cell", mos.grid_cellkringexplode("cell_id", F.lit(5)))\
  .join(
    nhda_area_df\
      .withColumn("join_cell", F.col("cell_id")),
    on = ["join_cell"]
  )\
  .select(
    roads_df["cell_id"],\
    "LINEARID", "ObjectID", "RTTYP", roads_df["geom"].alias("road_geom"),\
    "ftype", "elevation", nhda_area_df["geom"].alias("water_body") 
  ).join(
    precipitation_nyc_df,
    on = ["cell_id"]
  ).join(
    flood_risk_model.select("target", "cell_id")\
      .withColumn("cell_id", mos.grid_cellkringexplode("cell_id", F.lit(10))),
    on = ["cell_id"]
  ).join(
    bridges_df.select("CONDITION_RATING", "BRIDGE_LENGTH_FT")
  )

# COMMAND ----------

# MAGIC %md
# MAGIC Extract the features for the model (minimal subset to produce the baseline model).

# COMMAND ----------

features_df = train_df\
  .groupBy("cell_id")\
  .agg(
    F.avg("target").alias("target"),
    F.max("measure").alias("max_precipitation"),
    F.min("measure").alias("min_precipitation"),
    F.stddev("measure").alias("std_precipitation"),
    F.avg("elevation").alias("avg_elevation")
  )

# COMMAND ----------

features_df.display()

# COMMAND ----------

features_df.write.format("delta").saveAsTable("train_data")

# COMMAND ----------

features_df = spark.read.table("train_data")
features_df.count()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC features_df cell_id h3 50000

# COMMAND ----------


