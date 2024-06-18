# Databricks notebook source
# MAGIC %pip install databricks-mosaic

# COMMAND ----------

# MAGIC %md
# MAGIC # Process Open Street Maps data
# MAGIC
# MAGIC This notebook creates a [Delta Live Table](https://databricks.com/product/delta-live-tables) data pipeline that processes the OSM data ingested by the [0_Download](./0_Download) notebook.
# MAGIC
# MAGIC ![Process pipeline](https://raw.githubusercontent.com/databrickslabs/mosaic/main/notebooks/examples/python/OpenStreetMaps/Images/1_Process.png)
# MAGIC
# MAGIC ## Setup
# MAGIC
# MAGIC Go to `Workflows` -> `Delta Live Tables` -> `Create pipeline`
# MAGIC
# MAGIC ![create pipeline](https://raw.githubusercontent.com/databrickslabs/mosaic/main/notebooks/examples/python/OpenStreetMaps/Images/1_CreatePipelineDLT.png)
# MAGIC
# MAGIC * Select this notebook in the Notebook libraries
# MAGIC * Set the Target database name to `open_street_maps`
# MAGIC * Set the Pipeline mode to Triggered
# MAGIC * Set your desired cluster settings 
# MAGIC * Create the pipeline
# MAGIC * Run the pipeline
# MAGIC
# MAGIC Delta live tables will run the data transformations defined in this notebook and populate the tables in the target database.
# MAGIC
# MAGIC ![Pipeline](https://raw.githubusercontent.com/databrickslabs/mosaic/main/notebooks/examples/python/OpenStreetMaps/Images/1_Pipeline.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mosaic
# MAGIC
# MAGIC This workflow is using [Mosaic](https://github.com/databrickslabs/mosaic) to process the geospatial data.
# MAGIC
# MAGIC You can check out the documentation [here](https://databrickslabs.github.io/mosaic/).

# COMMAND ----------

import mosaic as mos

mos.enable_mosaic(spark, dbutils)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

raw_path = "dbfs:/tmp/mosaic/spd_osm_nl/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## DLT Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ### Input (bronze) tables

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.types import *

nodes = spark.read.format("delta").load(f"{raw_path}/bronze/nodes")

relations = spark.read.format("delta").load(f"{raw_path}/bronze/relations")

ways = spark.read.format("delta").load(f"{raw_path}/bronze/ways")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Lines and polygons

# COMMAND ----------

nodes = (
    nodes
    .withColumnRenamed("_id", "ref")
    .withColumnRenamed("_lat", "lat")
    .withColumnRenamed("_lon", "lon")
    .select("ref", "lat", "lon")
  )
    
lines = (
    ways
      # Join ways with nodes to get lat and lon for each node
      .select(
        f.col("_id").alias("id"),
        f.col("_timestamp").alias("ts"),
        f.posexplode_outer("nd")
      )
      .select("id", "ts", "pos", f.col("col._ref").alias("ref"))
      .join(nodes, "ref")
    
      # Collect an ordered list of points by way ID
      .withColumn("point", mos.st_point("lon", "lat"))
      .groupBy("id")
      .agg(f.collect_list(f.struct("pos", "point")).alias("points"))
      .withColumn("points", f.expr("transform(sort_array(points), x -> x.point)"))
      
      # Make and validate line
      .withColumn("line", mos.st_makeline("points"))
      .withColumn("is_valid", mos.st_isvalid("line"))
      .drop("points")
    )

  
line_roles = ["inner", "outer"]
  
polygons =(
    dlt.read("relations")

      .withColumnRenamed("_id", "id")
      .select("id", f.posexplode_outer("member"))
      .withColumn("ref", f.col("col._ref"))
      .withColumn("role", f.col("col._role"))
      .withColumn("member_type", f.col("col._type"))
      .drop("col")
      
       # Only inner and outer roles from ways.
      .filter(f.col("role").isin(line_roles))
      .filter(f.col("member_type") == "way")
    
      # Join with lines to get the lines
      .join(dlt.read("lines").select(f.col("id").alias("ref"), "line"), "ref")
      .drop("ref")
    
      # A valid polygon needs at least 3 vertices + a closing point
      .filter(f.size(f.col("line.boundary")[0]) >= 4)

      # Divide inner and outer lines
      .groupBy("id")
      .pivot("role", line_roles)
      .agg(f.collect_list("line"))

      # There should be exactly one outer line
      .filter(f.size("outer") == 1)
      .withColumn("outer", f.col("outer")[0])

      # Build the polygon
      .withColumn("polygon", mos.st_makepolygon("outer", "inner"))

      # check polygon validity
      .withColumn("is_valid", mos.st_isvalid("polygon"))
      
      # Drop extra columns
      .drop("inner", "outer")
  )
  
