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

# MAGIC %md
# MAGIC Because geospatial data is very particular:
# MAGIC * Each row of data isnt always equal to other data rows
# MAGIC * Individual rows require a lot of compute
# MAGIC * Small partitions are desirable for geospatial data </br>
# MAGIC
# MAGIC We should turn off the adaptive query execution otpimization in spark.

# COMMAND ----------

spark.conf.set("spark.databricks.optimizer.adaptive.enabled", "false")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE flood_risk

# COMMAND ----------

mupolygon_df = mos.read()\
  .format("multi_read_ogr")\
  .option("vsizip", "true")\
  .option("layerName", "mupolygon")\
  .load("dbfs:/geospatial/hazard_risk/soil-new/")\
  .repartition(200, F.col("Shape"))\
  .withColumn("geom", mos.st_updatesrid("Shape", "Shape_srid", F.lit(4326)))

mupolygon_df.display()

# COMMAND ----------

mupolygon_df.write.mode("overwrite").saveAsTable("mupolygon")

# COMMAND ----------

mupolygon_df = spark.read.table("mupolygon").drop("Shape")
mupolygon_df.display()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC mupolygon_df geom geometry 5000

# COMMAND ----------

mupolygon_df_tesselated = (
  mupolygon_df
    .repartition(200)
    .withColumn("grid", mos.grid_tessellateexplode("geom", F.lit(5)))
    .withColumn("geom", F.col("grid.wkb"))
    .repartition(200)
    .withColumn("grid", mos.grid_tessellateexplode("geom", F.lit(9)))
    .withColumn("cell_id", F.col("grid.index_id"))
    .withColumn("geom", F.col("grid.wkb"))
    .drop("grid")
)

# COMMAND ----------

mupolygon_df_tesselated.write.mode("overwrite").saveAsTable("mupolygon_tesselated")

# COMMAND ----------

mupolygon_df_tesselated = spark.read.table("mupolygon_tesselated")

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC mupolygon_df_tesselated geom geometry 2000

# COMMAND ----------

comonth_df = mos.read()\
  .format("multi_read_ogr")\
  .option("vsizip", "true")\
  .option("layerName", "comonth")\
  .load("dbfs:/geospatial/hazard_risk/soil-new/")\

comonth_df.display()

# COMMAND ----------

component_df = mos.read()\
  .format("multi_read_ogr")\
  .option("vsizip", "true")\
  .option("layerName", "component")\
  .load("dbfs:/geospatial/hazard_risk/soil-new/")\

component_df.display()

# COMMAND ----------

flood_risk_model_df = \
  comonth_df.join(component_df, on=["cokey"])\
  .join(mupolygon_df_tesselated, on=["mukey"])

# COMMAND ----------

flood_risk_model_df.write.mode("overwrite").saveAsTable("flood_risk_model")

# COMMAND ----------

flood_risk_model_df = spark.read.table("flood_risk_model")
flood_risk_model_df.display()

# COMMAND ----------

flood_risk_model_df.groupBy("flodfreqcl").count().display()

# COMMAND ----------

target_df = (
  flood_risk_model_df
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

target_df.display()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC target_df geom geometry 5000

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC target_df cell_id h3 50000

# COMMAND ----------


