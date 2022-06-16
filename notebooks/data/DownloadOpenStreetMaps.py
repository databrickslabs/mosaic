# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # INSTALL XML libraries first!
# MAGIC 
# MAGIC From your cluster settings install from Maven the xml reader library `com.databricks:spark-xml_2.12:0.14.0`
# MAGIC 
# MAGIC ## Setup temporary data location
# MAGIC 
# MAGIC We will setup a temporary location to store the Open Street Maps dataset </br>
# MAGIC 
# MAGIC 
# MAGIC https://wiki.openstreetmap.org/wiki/OSM_XML
# MAGIC 
# MAGIC https://wiki.openstreetmap.org/wiki/Elements

# COMMAND ----------

user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

raw_path = f"dbfs:/tmp/mosaic/{user_name}/open_street_maps/"

print(f"The raw data will be stored in {raw_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download Open Street Maps

# COMMAND ----------

import requests
import os
import pathlib
import pyspark.sql.functions as f
import mosaic as mos

mos.enable_mosaic(spark, dbutils)

# See available regions on https://download.geofabrik.de/
region = "italy"

# TODO: Extract all URLs from https://download.geofabrik.de/index-v1-nogeom.json
osm_url = f"https://download.geofabrik.de/europe/{region}/nord-est-latest.osm.bz2"

# The DBFS file system is mounted under /dbfs/ directory on Databricks cluster nodes

local_osm_path = pathlib.Path(raw_path.replace("dbfs:/", "/dbfs/"))
local_osm_path.mkdir(parents=True, exist_ok=True)

(local_osm_path / "raw").mkdir(parents=True, exist_ok=True)
# (local_osm_path / "raw_decompressed").mkdir(parents=True, exist_ok=True)

compressed_osm_file = local_osm_path / "raw" / f"{region}-latest.osm.bz2"
# decompressed_osm_file = local_osm_path / "raw_decompressed" / f"{region}-latest.osm"

# COMMAND ----------

req = requests.get(osm_url)
with open(compressed_osm_file, 'wb') as f:
  f.write(req.content)
  # TODO: We can decompress it on the fly

# COMMAND ----------

display(dbutils.fs.ls(raw_path + "raw"))

# COMMAND ----------

compressed_osm_file

# COMMAND ----------

# import bz2

# with open(decompressed_osm_file, 'wb') as new_file, bz2.BZ2File(compressed_osm_file, 'rb') as file:
#   for data in iter(lambda : file.read(100 * 1024), b''):
#     new_file.write(data)

# COMMAND ----------

# display(dbutils.fs.ls(raw_path + "/raw_decompressed/"))

# COMMAND ----------

from pyspark.sql.types import *

nodes_schema = StructType([
    StructField("_id", LongType(), True),
    StructField("_lat", DoubleType(), True),
    StructField("_lon", DoubleType(), True),
    StructField("_timestamp", TimestampType(), True),
    StructField("_visible", BooleanType(), True),
    StructField("_version", IntegerType(), True),
    StructField("tag", ArrayType(
      StructType([
         StructField("_VALUE", StringType(), True),
         StructField("_k", StringType(), True),
         StructField("_v", StringType(), True),
      ])
    ), True)])

nodes = (spark
      .read
      .format("xml")
      .options(rowTag="node") # Only extract nodes
      .load(f"{raw_path}/raw/", schema = nodes_schema)
     )

nodes.write.format("delta").mode("overwrite").save(f"{raw_path}/osm_bronze/nodes")
# nodes.display()

# COMMAND ----------

ways_schema = StructType([
    StructField("_id", LongType(), True),
    StructField("_timestamp", TimestampType(), True),
    StructField("_visible", BooleanType(), True),
    StructField("_version", IntegerType(), True),
    StructField("tag", ArrayType(
      StructType([
         StructField("_VALUE", StringType(), True),
         StructField("_k", StringType(), True),
         StructField("_v", StringType(), True),
      ])
    ), True),
    StructField("nd", ArrayType(
        StructType([
           StructField("_VALUE", StringType(), True),
           StructField("_ref", LongType(), True)
        ])
      ), True)
])

ways = (spark
      .read
      .format("xml")
      .options(rowTag="way") # Only extract ways
      .load(f"{raw_path}/raw/", schema = ways_schema)
     )

ways.write.format("delta").mode("overwrite").save(f"{raw_path}/osm_bronze/ways")

# COMMAND ----------

relations_schema = StructType([
    StructField("_id", LongType(), True),
    StructField("_timestamp", TimestampType(), True),
    StructField("_visible", BooleanType(), True),
    StructField("_version", IntegerType(), True),
    StructField("tag", ArrayType(
      StructType([
         StructField("_VALUE", StringType(), True),
         StructField("_k", StringType(), True),
         StructField("_v", StringType(), True),
      ])
    ), True),
    StructField("member", ArrayType(
      StructType([
         StructField("_VALUE", StringType(), True),
         StructField("_ref", LongType(), True),
         StructField("_role", StringType(), True),
         StructField("_type", StringType(), True),
      ])
      ), True)
])

relations = (spark
      .read
      .format("xml")
      .options(rowTag="relation") # Only extract relations
      .load(f"{raw_path}/raw/", schema=relations_schema)
     )
relations.write.format("delta").mode("overwrite").save(f"{raw_path}/osm_bronze/relations")

# COMMAND ----------

nodes = spark.read.format("delta").load(f"{raw_path}/osm_bronze/nodes")
relations = spark.read.format("delta").load(f"{raw_path}/osm_bronze/relations")
ways = spark.read.format("delta").load(f"{raw_path}/osm_bronze/ways")


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## TODO: 
# MAGIC 
# MAGIC * Check overpass API for the POC
# MAGIC * Add check for lat-long min max

# COMMAND ----------

relations.groupBy("_visible").count().display()

# COMMAND ----------



nodes_clean = (nodes
               .withColumnRenamed("_id", "id")
               .withColumnRenamed("_lat", "lat")
               .withColumnRenamed("_lon", "lon")
               .withColumnRenamed("_timestamp", "ts")
               .drop("_visible", "_version")
#                .filter(f.col("tag").isNotNull())
#                .select("id", "lat", "lon", "ts", f.explode_outer("tag"))
#                .withColumn("key", f.col("col._k"))
#                .withColumn("val", f.col("col._v"))
#                .drop("col")
              )

nodes_clean.write.format("delta").mode("overwrite").save(f"{raw_path}/osm_silver/nodes_clean")

# COMMAND ----------

keys_of_interes = ["type", "restriction", "landuse", "name", "network", "route", "ref", "building", "source", "operator", "natural", "website"]

relations_tags = (relations
               .withColumnRenamed("_id", "id")
               .select("id", f.explode_outer("tag"))
               .withColumn("key", f.col("col._k"))
               .withColumn("val", f.col("col._v"))
               .groupBy("id")
               .pivot("key", keys_of_interes)
               .agg(f.first("val"))
              )

relations_tags.write.format("delta").mode("overwrite").save(f"{raw_path}/osm_silver/relations_tags")

# COMMAND ----------

relations_tags = spark.read.format("delta").load(f"{raw_path}/osm_silver/relations_tags")
nodes_clean = spark.read.format("delta").load(f"{raw_path}/osm_silver/nodes_clean")


# COMMAND ----------

ways_coords = (ways
              .select(
                f.col("_id").alias("id"),
                f.col("_timestamp").alias("ts"),
                f.posexplode_outer("nd")
              )
               .select("id", "ts", "pos", f.col("col._ref").alias("ref"))
               .join(nodes_clean.select(f.col("id").alias("ref"), "lat", "lon"), "ref")
               
             )

# ways_coords.display()
ways_coords.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{raw_path}/osm_silver/ways_coords")

# COMMAND ----------

ways_coords = spark.read.format("delta").load(f"{raw_path}/osm_silver/ways_coords")


# COMMAND ----------

from pyspark.sql.window import Window


way_lines = (ways_coords
             .repartition("id")
             .sortWithinPartitions("pos")
             .withColumn("point", mos.st_point("lon", "lat"))
             .groupBy("id")
             .agg(f.collect_list("point").alias("points"))
             .withColumn("line", mos.st_makeline("points"))
             .drop("points")
            )

way_lines.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{raw_path}/osm_silver/way_lines")
# way_lines.display()

# COMMAND ----------

way_lines = spark.read.format("delta").load(f"{raw_path}/osm_silver/way_lines")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simple Buildings (Not multi-ploygons)

# COMMAND ----------

ways_tags = (ways
            .select(
              f.col("_id").alias("id"),
              f.explode_outer("tag")
             )
             .select(f.col("id"), f.col("col._k").alias("key"), f.col("col._v").alias("value"))
            )
  
ways_tags.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{raw_path}/osm_silver/ways_tags")


# COMMAND ----------

ways_tags = spark.read.format("delta").load(f"{raw_path}/osm_silver/ways_tags")

# COMMAND ----------

simple_buildings = (way_lines
                    .join(ways_tags.filter(f.col("key") == "building"), "id")
                    .withColumnRenamed("value", "building")
                    
                    # A valid polygon needs at least 3 vertices + a closing point
                    .filter(f.size(f.col("line.boundary")[0]) >= 4)
                           
                     # Build the polygon
                     .withColumn("polygon", mos.st_makepolygon("line"))

#                      # Filter valid polygons
#                      .withColumn("is_valid", f.expr("try_sql(st_isvalid(polygon))"))
#                      .filter(f.col("is_valid.status") == "OK")
#                      .filter(f.col("is_valid.result"))
                    .drop("line", "key")
                   )

simple_buildings.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{raw_path}/osm_silver/simple_buildings")
# simple_buildings.display()

# COMMAND ----------

simple_buildings = spark.read.format("delta").load(f"{raw_path}/osm_silver/simple_buildings")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Multipolygons (from relations)

# COMMAND ----------

relations_members = (relations
                     .withColumnRenamed("_id", "id")
                     .select("id", f.posexplode_outer("member"))
                     .withColumn("ref", f.col("col._ref"))
                     .withColumn("role", f.col("col._role"))
                     .withColumn("member_type", f.col("col._type"))
                     .drop("col")
                    )

relations_members.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{raw_path}/osm_silver/relations_members")

# COMMAND ----------

relations_members = spark.read.format("delta").load(f"{raw_path}/osm_silver/relations_members")

# COMMAND ----------

relations_members.groupBy("id").count().join(relations_tags, "id").orderBy(f.col("count").desc()).display()

# COMMAND ----------

relations_polygons = (relations_members
                      .filter(f.col("member_type") == "way")
                      .join(relations_tags.filter(f.col("type") == "multipolygon"), "id")
                      .join(way_lines.select(f.col("id").alias("ref"), "line"), "ref")
                      .drop("ref")
              )

relations_polygons.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{raw_path}/osm_silver/relations_polygons")


# COMMAND ----------

relations_polygons = spark.read.format("delta").load(f"{raw_path}/osm_silver/relations_polygons")

# COMMAND ----------

(relations_polygons
   # Only inner and outer. There might be none types
   .filter(f.col("role").isin("outer", "inner"))

   # A valid polygon needs at least 3 vertices + a closing point
   .filter(f.size(f.col("line.boundary")[0]) >= 4)

   .groupBy("id")
   .pivot("role")
   .agg(f.collect_list("line"))

    .select(f.size("outer"), f.size("inner"), f.col("id"))
).display()

# TODO: fix polygons with multiple outer lines

# COMMAND ----------

relations_multipolygons = (relations_polygons
                           # Only inner and outer. There might be none types
                           .filter(f.col("role").isin("inner", "outer"))
                           
                           # A valid polygon needs at least 3 vertices + a closing point
                           .filter(f.size(f.col("line.boundary")[0]) >= 4)
                           
                           # Divide inner and outer lines
                           .groupBy("id")
                           .pivot("role")
                           .agg(f.collect_list("line"))
                           
                           # There should be exactly one outer line
                           .filter(f.size("outer") == 1)
                           .withColumn("outer", f.col("outer")[0])
                           
                           # Build the polygon
                           .withColumn("polygon", mos.st_makepolygon("outer", "inner"))
                           
                           # Filter valid polygons
                           .withColumn("is_valid", f.expr("try_sql(st_isvalid(polygon))"))
                           .filter(f.col("is_valid.status") == "OK")
                           .filter(f.col("is_valid.result"))
                           
                           # Drop extra columns
                           .drop("inner", "outer", "is_valid")
              )

relations_multipolygons.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{raw_path}/osm_silver/relations_multipolygons")
# relations_multipolygons.display()

# COMMAND ----------

relations_multipolygons = spark.read.format("delta").load(f"{raw_path}/osm_silver/relations_multipolygons")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore the data

# COMMAND ----------

tmp = (relations_multipolygons
       .union(simple_buildings.select("id", "polygon"))
       .withColumn("centroid", mos.st_centroid2D("polygon"))
       
       .cache()
       
       # Trieste
       .filter(f.col("centroid.x").between(13.7399, 13.803))
       .filter(f.col("centroid.y").between(45.6106, 45.6436))
       
       
#        .withColumn("centroid", f.expr("try_sql(st_centroid2D(polygon))"))

#        .withColumn("h3", mos.point_index_lonlat("centroid.x", "centroid.y", f.lit(7)))
#        .join(relations_tags, "id").filter(f.col("building").isNull())
       
#        .filter(f.col("h3") == 608540806336217087)
       
#        .select("centroid", "polygon")
      )
tmp.display()

# COMMAND ----------

tmp.count()

# COMMAND ----------

# MAGIC %python
# MAGIC %%mosaic_kepler
# MAGIC tmp "polygon" "geometry" 10000

# COMMAND ----------

# MAGIC %python
# MAGIC %%mosaic_kepler
# MAGIC tmp "polygon" "geometry" 1000

# COMMAND ----------


# from shapely.geometry import Polygon
# import matplotlib.pyplot as plt


# import numpy as np
# from matplotlib.path import Path
# from matplotlib.patches import PathPatch
# from matplotlib.collections import PatchCollection


# # Plots a Polygon to pyplot `ax`
# def plot_polygon(ax, poly, **kwargs):
#     path = Path.make_compound_path(
#         Path(np.asarray(poly.exterior.coords)[:, :2]),
#         *[Path(np.asarray(ring.coords)[:, :2]) for ring in poly.interiors])

#     patch = PathPatch(path, **kwargs)
#     collection = PatchCollection([patch], **kwargs)
    
#     ax.add_collection(collection, autolim=True)
#     ax.autoscale_view()
#     return collection
  

# # Input polygon with two holes
# # (remember exterior point order is ccw, holes cw else
# # holes may not appear as holes.)

# fig, ax = plt.subplots()
# plot_polygon(ax, polygon, facecolor='lightblue', edgecolor='red')

# COMMAND ----------


