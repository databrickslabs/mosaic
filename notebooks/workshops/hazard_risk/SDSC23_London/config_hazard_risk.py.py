# Databricks notebook source
# https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution
spark.conf.set("spark.sql.adaptive.coalescePartitions.parallelismFirst", False)        # <-- default is true (respect size)
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "24MB")              # <-- default is 64MB
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", 2)                 # <-- default is 5
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "64MB")  # <-- default is 256MB
spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", 10_000)    # <-- default is shuffle setting (200)
 
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", '2147483648b')          # <- default is '10485760b' or 10m ('2147483648b' for 2gb | '0b')
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", '2147483648b') # <- default is ''
 
spark.conf.set("spark.databricks.delta.properties.defaults.targetFileSize", '50331648') # default is not set ('50331648' or 48MB) vs ~128MB
spark.conf.set("spark.databricks.delta.properties.defaults.tuneFileSizesForRewrites", True) # default is not set 

print("""
# https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution
spark.conf.set("spark.sql.adaptive.coalescePartitions.parallelismFirst", False)        # <-- default is true (respect size)
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "24MB")              # <-- default is 64MB
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", 2)                 # <-- default is 5
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "64MB")  # <-- default is 256MB
spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", 10_000)    # <-- default is shuffle setting (200)
 
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", '2147483648b')          # <- default is '10485760b' or 10m ('2147483648b' for 2gb | '0b')
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", '2147483648b') # <- default is ''
 
spark.conf.set("spark.databricks.delta.properties.defaults.targetFileSize", '50331648') # default is not set ('50331648' or 48MB) vs ~128MB
spark.conf.set("spark.databricks.delta.properties.defaults.tuneFileSizesForRewrites", True) # default is not set 
""")

# COMMAND ----------

import os
import pprint

from pyspark.databricks.sql.functions import *

from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.types import *
from pyspark.sql import Window

print("""
import os
import pprint

from pyspark.databricks.sql.functions import *

from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.types import *
from pyspark.sql import Window
""")

# COMMAND ----------

os.environ['HAZARD_RISK_DBFS'] = "/geospatial/hazard_risk"
os.environ['HAZARD_RISK_FUSE'] = f"/dbfs{os.environ['HAZARD_RISK_DBFS']}" 

HAZARD_RISK_DBFS = os.environ['HAZARD_RISK_DBFS']
HAZARD_RISK_FUSE = os.environ['HAZARD_RISK_FUSE']

print(f"HAZARD_RISK_DBFS: '{os.environ['HAZARD_RISK_DBFS']}', HAZARD_RISK_FUSE: '{os.environ['HAZARD_RISK_FUSE']}' (DBFS created)")

# COMMAND ----------

# MAGIC %sh mkdir -p $HAZARD_RISK_FUSE

# COMMAND ----------

db_name = "hazard_risk"
sql(f"""create database if not exists {db_name}""")
sql(f"""use database {db_name}""")

print(f"database `db_name`: '{db_name}' (created)")

# COMMAND ----------

# MAGIC %md __Shapefile Reader (with PyShp)__
# MAGIC
# MAGIC > Note: offers `shapefile_reader` for single (ZIP) path.

# COMMAND ----------

def shapefile_as_json(shapefile_path:str, java_friendly:bool=True):
  """
  Read shapefile (focused on zip file)
  - flattens properties and geometry into records
  - java_friendly will handle converting:
    1. from single quotes to double
    2. from tuples to lists
  Return json list of maps
  """
  import shapefile
  
  with shapefile.Reader(shapefile_path) as shp:
      shape_records = []

      # Iterate through each shape record
      for shape in shp.shapeRecords():
        shape_record = shape.record.as_dict() # Read record
        geojson = {'geojson':shape.shape.__geo_interface__.__str__()} # Read shapefile GeoJSON
        if java_friendly:
          geojson['geojson']  = geojson['geojson'].replace("'",'"').replace("(","[").replace(")","]")
        shape_records.append({**shape_record, **geojson}) # Concatenate and append
  
  return(shape_records)

print("""
def shapefile_as_json(shapefile_path:str, java_friendly:bool=True):
  Read shapefile (focused on zip file)
  - flattens properties and geometry as geojson
  - java_friendly will handle converting:
    1. from single quotes to double
    2. from tuples to lists
  Return json list of maps
""")

# COMMAND ----------

def shapefile_as_geojson(shapefile_path:str):
  """
  Read shapefile (focused on zip file)
  - properties and geometry as geojson
  Return geojson as string
  """
  import shapefile
  import json
  
  # see https://gist.github.com/frankrowe/6071443
  with shapefile.Reader(shapefile_path) as shp: 
    fields = shp.fields[1:]
    field_names = [field[0] for field in fields]
    buffer = []
    
    for sr in shp.shapeRecords():
      attr = dict(zip(field_names, sr.record))
      geom = sr.shape.__geo_interface__
      buffer.append(dict(type="Feature", geometry=geom, properties=attr)) 
    
    return json.dumps(
      {"type": "FeatureCollection", "features": buffer}, 
      indent=2
    )

print("""
def shapefile_as_geojson(shapefile_path:str):
  Read shapefile (focused on zip file)
  - properties and geometry as geojson
  Return geojson as string
""")

# COMMAND ----------

# MAGIC %md __Convenience Functions__
# MAGIC
# MAGIC > Note: useful for storing and registering delta lake tables.

# COMMAND ----------

def is_dbfs_dir_exists(dbfs_dir, debug=True):
  """
  Convienence to verify if a dbfs dir exists.
  Returns True | False
  """
  exists = False
  try:
    if len(dbutils.fs.ls(dbfs_dir)) > 0:
      exists = True
  except Exception:
    print(f"...'{dbfs_dir}' does not exist.") if debug==True else None
    pass
  return exists

print("""
def is_dbfs_dir_exists(dbfs_dir, debug=True):
  Convienence to verify if a dbfs dir exists.
  Returns True | False
""")

# COMMAND ----------

def handle_delta_optimize_register(delta_dir, tbl_name, db_name, zorder_col=None, register_table=True):
  """
  """
  # -- Optimze + Z-Order
  if zorder_col is not None:
    sql(f"""OPTIMIZE delta.`{delta_dir}` ZORDER BY ({zorder_col})""")
  
  # -- Register Table
  if register_table:
    sql(f"""DROP TABLE IF EXISTS {db_name}.{tbl_name}""") # <-- register table with metastore
    sql(f"""CREATE TABLE {db_name}.{tbl_name} 
                USING DELTA
                LOCATION '{delta_dir}'
        """)
    return spark.table(f"{db_name}.{tbl_name}")
  else:
    return spark.read.load(delta_dir)

print("""
def handle_delta_optimize_register(delta_dir, tbl_name, db_name, zorder_col=None, register_table=True)
""")

# COMMAND ----------

def write_to_delta(_df, delta_dir, tbl_name, db_name, zorder_col=None, register_table=True, overwrite=False, debug=True):
  """
  - (1) write dataframe to delta lake
  - (2) optionally: optimize + z-order
  - (3) register table
  This uses various existing variables as well.
  """
  exists = is_dbfs_dir_exists(delta_dir)
  if exists and not overwrite:
    print(f"...returning, '{delta_dir}' exists and is not empty and overwrite=False") if debug==True else None
    if register_table:
      return spark.table(f"{db_name}.{tbl_name}")
    else:
      return spark.read.load(delta_dir)
  
  # -- Write to Delta Lake
  dbutils.fs.rm(delta_dir, True)
  (
    _df
      .write
        .mode("overwrite")
        .option("mergeSchema", "true")
      .save(delta_dir)
  )
  
  return handle_delta_optimize_register(delta_dir, tbl_name, db_name, zorder_col=zorder_col, register_table=register_table)

print("""
def write_to_delta(_df, delta_dir, tbl_name, db_name, zorder_col=None, register_table=True, overwrite=False, debug=True):
  - (1) write dataframe to delta lake
  - (2) optionally: optimize + z-order
  - (3) register table
  This uses various existing variables as well.
""")
