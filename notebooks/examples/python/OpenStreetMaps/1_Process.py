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

raw_path = f"dbfs:/tmp/mosaic/open_street_maps/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## DLT Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ### Input (bronze) tables

# COMMAND ----------

import dlt
import pyspark.sql.functions as f
from pyspark.sql.types import *

@dlt.table(comment="OpenStreetMaps nodes")
def nodes():
  return spark.read.format("delta").load(f"{raw_path}/bronze/nodes")


@dlt.table(comment="OpenStreetMaps relations")
def relations():
  return spark.read.format("delta").load(f"{raw_path}/bronze/relations")


@dlt.table(comment="OpenStreetMaps ways")
def ways():
  return spark.read.format("delta").load(f"{raw_path}/bronze/ways")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Lines and polygons

# COMMAND ----------

@dlt.table()
@dlt.expect("is_valid", "is_valid")
def lines():
  
  # Pre-select node columns
  nodes = (
    dlt.read("nodes")
    .withColumnRenamed("_id", "ref")
    .withColumnRenamed("_lat", "lat")
    .withColumnRenamed("_lon", "lon")
    .select("ref", "lat", "lon")
  )
    
  return (
    dlt.read("ways")
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

  
@dlt.table()
@dlt.expect_or_drop("is_valid", "is_valid")
def polygons():
  line_roles = ["inner", "outer"]
  
  return (
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
  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Buildings

# COMMAND ----------

def get_simple_buildings():
  
  building_tags = (dlt.read("ways")
      .select(
        f.col("_id").alias("id"),
        f.explode_outer("tag")
       )
       .select(f.col("id"), f.col("col._k").alias("key"), f.col("col._v").alias("value"))
       .filter(f.col("key") == "building")
  )
      
  return (
    dlt.read("lines")
      .join(building_tags, "id")
      .withColumnRenamed("value", "building")

      # A valid polygon needs at least 3 vertices + a closing point
      .filter(f.size(f.col("line.boundary")[0]) >= 4)

      # Build the polygon
      .withColumn("polygon", mos.st_makepolygon("line"))

      .withColumn("is_valid", mos.st_isvalid("polygon"))
      .drop("line", "key")
     )

def get_complex_buildings():
  keys_of_interes = ["type", "building"]
  
  relation_tags = (dlt.read("relations")
      .withColumnRenamed("_id", "id")
      .select("id", f.explode_outer("tag"))
      .withColumn("key", f.col("col._k"))
      .withColumn("val", f.col("col._v"))
      .groupBy("id")
      .pivot("key", keys_of_interes)
      .agg(f.first("val"))
  )
  
  return (
    dlt.read("polygons")
      .join(relation_tags, "id")
      
      # Only get polygons
      .filter(f.col("type") == "multipolygon")
    
      # Only get buildings
      .filter(f.col("building").isNotNull())
  )

@dlt.table()
def buildings():
  fields = ["id", "building", "polygon"]
  complex_buildings = get_complex_buildings().select(fields)
  simple_buildings = get_simple_buildings().select(fields)
  
  return complex_buildings.union(simple_buildings)
  
  
@dlt.table()
def buildings_indexed():
  return (
    dlt.read("buildings")
      .withColumn("centroid", mos.st_centroid2D("polygon"))
      .withColumn("centroid_index_res_5", mos.grid_longlatascellid("centroid.x", "centroid.y", f.lit(5)))
      .withColumn("centroid_index_res_6", mos.grid_longlatascellid("centroid.x", "centroid.y", f.lit(6)))
      .withColumn("centroid_index_res_7", mos.grid_longlatascellid("centroid.x", "centroid.y", f.lit(7)))
      .withColumn("centroid_index_res_8", mos.grid_longlatascellid("centroid.x", "centroid.y", f.lit(8)))
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Building types

# COMMAND ----------

@dlt.table()
def residential_buildings():
  return (
    dlt.read("buildings_indexed")
      .filter(f.col("building").isin(["yes", "residential", "house", "apartments"]))
  )
  
@dlt.table()
def hospital_buildings():
  return (
    dlt.read("buildings_indexed")
      .filter(f.col("building").isin(["hospital"]))
  )
  
@dlt.table()
def university_buildings():
  return (
    dlt.read("buildings_indexed")
      .filter(f.col("building").isin(["university"]))
  )

@dlt.table()
def train_station_buildings():
  return (
    dlt.read("buildings_indexed")
      .filter(f.col("building").isin(["train_station"]))
  )
