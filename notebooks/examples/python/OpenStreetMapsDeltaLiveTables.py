# Databricks notebook source
# MAGIC %pip install https://github.com/databrickslabs/mosaic/releases/download/v0.1.1/databricks_mosaic-0.1.1-py3-none-any.whl

# COMMAND ----------

import mosaic as mos

mos.enable_mosaic(spark, dbutils)

# COMMAND ----------

import dlt
import pyspark.sql.functions as f
from pyspark.sql.types import *

user_name = "erni.durdevic@databricks.com" #dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

raw_path = f"dbfs:/tmp/mosaic/{user_name}/open_street_maps/"


@dlt.table(comment="OpenStreetMaps nodes")
def nodes():
  return spark.read.format("delta").load(f"{raw_path}/osm_bronze/nodes")


@dlt.table(comment="OpenStreetMaps relations")
def relations():
  return spark.read.format("delta").load(f"{raw_path}/osm_bronze/relations")


@dlt.table(comment="OpenStreetMaps ways")
def ways():
  return spark.read.format("delta").load(f"{raw_path}/osm_bronze/ways")


    
@dlt.view()
def nodes_clean():
  return (
    dlt.read("nodes")
      .withColumnRenamed("_id", "id")
      .withColumnRenamed("_lat", "lat")
      .withColumnRenamed("_lon", "lon")
      .withColumnRenamed("_timestamp", "ts")
      .drop("_visible", "_version")
  )
  
@dlt.table()
def ways_coords():
  return (
    dlt.read("ways")
      .select(
        f.col("_id").alias("id"),
        f.col("_timestamp").alias("ts"),
        f.posexplode_outer("nd")
      )
      .select("id", "ts", "pos", f.col("col._ref").alias("ref"))
      .join(dlt.read("nodes_clean").select(f.col("id").alias("ref"), "lat", "lon"), "ref")
     )

@dlt.table()
@dlt.expect("is_valid", "is_valid")
def way_lines():
  return (
    dlt.read("ways_coords")
             .repartition("id")
             .sortWithinPartitions("pos")
             .withColumn("point", mos.st_point("lon", "lat"))
             .groupBy("id")
             .agg(f.collect_list("point").alias("points"))
             .withColumn("line", mos.st_makeline("points"))
             .withColumn("is_valid", mos.st_isvalid("line"))
             .drop("points")
            )

@dlt.table()
def ways_tags():
  return (
    dlt.read("ways")
      .select(
        f.col("_id").alias("id"),
        f.explode_outer("tag")
       )
       .select(f.col("id"), f.col("col._k").alias("key"), f.col("col._v").alias("value"))
    )

@dlt.table()
@dlt.expect_or_drop("is_valid", "is_valid")
def simple_buildings():
  return (
    dlt.read("way_lines")
      .join(dlt.read("ways_tags").filter(f.col("key") == "building"), "id")
      .withColumnRenamed("value", "building")

      # A valid polygon needs at least 3 vertices + a closing point
      .filter(f.size(f.col("line.boundary")[0]) >= 4)

      # Build the polygon
      .withColumn("polygon", mos.st_makepolygon("line"))

      .withColumn("is_valid", mos.st_isvalid("polygon"))
      .drop("line", "key")
     )
  
  
# Complex polygon objects (using relations)


@dlt.table(comment="relation tags")
def relation_tags():
  keys_of_interes = ["type", "restriction", "landuse", "name", "network", "route", "ref", "building", "source", "operator", "natural", "website"]
  return (
    dlt.read("relations")
      .withColumnRenamed("_id", "id")
      .select("id", f.explode_outer("tag"))
      .withColumn("key", f.col("col._k"))
      .withColumn("val", f.col("col._v"))
      .groupBy("id")
      .pivot("key", keys_of_interes)
      .agg(f.first("val"))
    )
  
@dlt.table()
def relation_lines():
  return (
    dlt.read("relations")

      .withColumnRenamed("_id", "id")
      .select("id", f.posexplode_outer("member"))
      .withColumn("ref", f.col("col._ref"))
      .withColumn("role", f.col("col._role"))
      .withColumn("member_type", f.col("col._type"))
      .drop("col")
      
      .filter(f.col("member_type") == "way")
      .join(dlt.read("way_lines").select(f.col("id").alias("ref"), "line"), "ref")
      .drop("ref")
    )
  
@dlt.table()
@dlt.expect_or_drop("is_valid", "is_valid")
def relation_polygons():
  line_roles = ["inner", "outer"]
  return (
    dlt.read("relation_lines")
#       .join(dlt.read("relation_tags"), "id")
    
#       # Only get polygons
#       .filter(f.col("type") == "multipolygon")
    
      # Only inner and outer roles. There might be none types
      .filter(f.col("role").isin(line_roles))

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
  
dlt.table()
def complex_buildings():
  return (
    dlt.read("relation_polygons")
      .join(dlt.read("relation_tags"), "id")
      
      # Only get polygons
      .filter(f.col("type") == "multipolygon")
      # Only get buildings
      .filter(f.col("building").isNotNull())
  )
  
dlt.table()
def buildings():
  fields = ["id", "building", "polygon"]
  return (
    dlt.read("complex_buildings")
      .select(fields)
      .union(dlt.read("simple_buildings").select(fields))
  )
  

# COMMAND ----------


