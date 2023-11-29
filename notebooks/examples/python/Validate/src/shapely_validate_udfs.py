# Databricks notebook source
# MAGIC %md # Shapely Validate Example 
# MAGIC
# MAGIC > Parallel handling of of a mixture of valid and invalid geometries using [regular](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udf.html?highlight=udf#pyspark.sql.functions.udf) and [vectorized pandas](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.pandas_udf.html?highlight=pandas%20udf#pyspark.sql.functions.pandas_udf) UDFs.
# MAGIC
# MAGIC __Libraries__
# MAGIC
# MAGIC <p/>
# MAGIC
# MAGIC * 'databricks-mosaic' (installs geopandas and dependencies as well as keplergl)
# MAGIC
# MAGIC --- 
# MAGIC  __Last Update__ 22 NOV 2023 [Mosaic 0.3.12]

# COMMAND ----------

# MAGIC %md ## Setup

# COMMAND ----------

# MAGIC %md ### Imports

# COMMAND ----------

# MAGIC %pip install "databricks-mosaic<0.4,>=0.3" --quiet # <- Mosaic 0.3 series
# MAGIC # %pip install "databricks-mosaic<0.5,>=0.4" --quiet # <- Mosaic 0.4 series (as available)

# COMMAND ----------

# -- configure AQE for more compute heavy operations
#  - choose option-1 or option-2 below, essential for REPARTITION!
# spark.conf.set("spark.databricks.optimizer.adaptive.enabled", False) # <- option-1: turn off completely for full control
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False) # <- option-2: just tweak partition management
spark.conf.set("spark.sql.shuffle.partitions", 10_000)                 # <-- default is 200

# -- import databricks + spark functions
from pyspark.databricks.sql import functions as dbf
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.types import *

# -- setup mosaic
import mosaic as mos

mos.enable_mosaic(spark, dbutils)
# mos.enable_gdal(spark) # <- not needed for this example

# --other imports
import geopandas as gpd
import json
import matplotlib.pyplot as plt
import shapely
import warnings

warnings.simplefilter("ignore")

# COMMAND ----------

# MAGIC %md ### Data
# MAGIC
# MAGIC > Generating a dataset with some bad data, adapted from [here](https://github.com/kleunen/boost_geometry_correct).
# MAGIC
# MAGIC These are the types of issues that can come up with geometries [[1](https://stackoverflow.com/questions/49902090/dataset-of-invalid-geometries-in-boostgeometry)]...
# MAGIC
# MAGIC ```
# MAGIC //Hole Outside Shell
# MAGIC check("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))");
# MAGIC //Nested Holes
# MAGIC check("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2), (3 3, 3 7, 7 7, 7 3, 3 3))");
# MAGIC //Disconnected Interior
# MAGIC check("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (5 0, 10 5, 5 10, 0 5, 5 0))");
# MAGIC //Self Intersection
# MAGIC check("POLYGON((0 0, 10 10, 0 10, 10 0, 0 0))");
# MAGIC //Ring Self Intersection
# MAGIC check("POLYGON((5 0, 10 0, 10 10, 0 10, 0 0, 5 0, 3 3, 5 6, 7 3, 5 0))");
# MAGIC //Nested Shells
# MAGIC check<multi>("MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)),(( 2 2, 8 2, 8 8, 2 8, 2 2)))");
# MAGIC //Duplicated Rings
# MAGIC check<multi>("MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)),((0 0, 10 0, 10 10, 0 10, 0 0)))");
# MAGIC //Too Few Points
# MAGIC check("POLYGON((2 2, 8 2))");
# MAGIC //Invalid Coordinate
# MAGIC check("POLYGON((NaN 3, 3 4, 4 4, 4 3, 3 3))");
# MAGIC //Ring Not Closed
# MAGIC check("POLYGON((0 0, 0 10, 10 10, 10 0))");
# MAGIC ```

# COMMAND ----------

test_wkts = []

# COMMAND ----------

# MAGIC %md __[1a] Polygon self-intersection__
# MAGIC
# MAGIC > Exterior xy plot with shapely (to see the lines).

# COMMAND ----------

test_wkts.append((1, """POLYGON ((5 0, 2.5 9, 9.5 3.5, 0.5 3.5, 7.5 9, 5 0))"""))

# COMMAND ----------

plt.plot(*shapely.wkt.loads(test_wkts[0][1]).exterior.xy)

# COMMAND ----------

# MAGIC %md __[1b] Polygon with hole inside__
# MAGIC
# MAGIC > Exterior xy plot with shapely (to see the lines).

# COMMAND ----------

test_wkts.append((2, """POLYGON ((55 10, 141 237, 249 23, 21 171, 252 169, 24 89, 266 73, 55 10))"""))

# COMMAND ----------

plt.plot(*shapely.wkt.loads(test_wkts[1][1]).exterior.xy)

# COMMAND ----------

# MAGIC %md __[1c] Polygon with multiple intersections at same point__
# MAGIC
# MAGIC > Exterior xy plot with shapely (to see the lines).

# COMMAND ----------

test_wkts.append((3, """POLYGON ((0 0, 10 0, 0 10, 10 10, 0 0, 5 0, 5 10, 0 10, 0 5, 10 5, 10 0, 0 0))"""))

# COMMAND ----------

plt.plot(*shapely.wkt.loads(test_wkts[2][1]).exterior.xy)

# COMMAND ----------

# MAGIC %md __[1d] Valid Polygon__

# COMMAND ----------

test_wkts.append((4, """POLYGON (( -84.3641541604937 33.71316821215546, -84.36414611386687 33.71303657522174, -84.36409515189553 33.71303657522174, -84.36410319852232 33.71317267442025, -84.3641541604937 33.71316821215546 ))"""))

# COMMAND ----------

plt.plot(*shapely.wkt.loads(test_wkts[3][1]).exterior.xy)

# COMMAND ----------

# MAGIC %md __[2] Make Spark DataFrame from `test_wkts`__

# COMMAND ----------

df = (
  spark
    .createDataFrame(test_wkts, schema=['row_id', 'geom_wkt'])
)
print(f"count? {df.count():,}")
df.display()

# COMMAND ----------

# MAGIC %md ## Regular UDF: Test + Fix Validity
# MAGIC
# MAGIC > Will use Mosaic to initially test; then only apply UDF to invalids

# COMMAND ----------

# MAGIC %md ### UDFs

# COMMAND ----------

@udf(returnType=StringType())
def explain_wkt_validity(geom_wkt:str) -> str:
    """
    Add explanation of validity or invalidity
    """
    from shapely import wkt
    from shapely.validation import explain_validity

    _geom = wkt.loads(geom_wkt)
    return explain_validity(_geom)


@udf(returnType=StringType())
def make_wkt_valid(geom_wkt:str) -> str:
    """
    - test for wkt being valid
    - attempts to make valid
    - may have to change type, e.g. POLYGON to MULTIPOLYGON
     returns valid wkt
    """
    from shapely import wkt 
    from shapely.validation import make_valid

    _geom = wkt.loads(geom_wkt)
    if _geom.is_valid:
        return geom_wkt
    _geom_fix = make_valid(_geom)
    return _geom_fix.wkt

# COMMAND ----------

# MAGIC %md ### Test Validity

# COMMAND ----------

df_test_valid = (
  df
    .withColumn("is_valid", mos.st_isvalid("geom_wkt"))
)

df_test_valid.display()

# COMMAND ----------

# MAGIC %md __Let's get an explanation for our 3 invalids__

# COMMAND ----------

# MAGIC %md _Recommend `explain_wkt_valid` only to help you understand, not as part of production pipeline, so doing separately._

# COMMAND ----------

display(
  df_test_valid
  .select(
    "*",
    F
      .when(col("is_valid") == False, explain_wkt_validity("geom_wkt"))
      .otherwise(F.lit(None))
      .alias("invalid_explain")
  )
  .filter("is_valid = false")
)

# COMMAND ----------

# MAGIC %md ### Fix Validity

# COMMAND ----------

df_valid = (
  df
    .withColumnRenamed("geom_wkt", "orig_geom_wkt")
    .withColumn("is_orig_valid", mos.st_isvalid("orig_geom_wkt"))
  .select(
    "*",
    F
      .when(col("is_orig_valid") == False, make_wkt_valid("orig_geom_wkt"))
      .otherwise(col("orig_geom_wkt"))
      .alias("geom_wkt")
  )
  .withColumn("is_valid", mos.st_isvalid("geom_wkt"))
  .drop("orig_geom_wkt")
)

print(f"""count? {df_valid.count():,}""")
print(f"""num orig invalid? {df_valid.filter(col("is_orig_valid") == False).count():,}""")
print(f"""num final invalid? {df_valid.filter(col("is_valid") == False).count():,}""")
display(df_valid)

# COMMAND ----------

fix_wkts = df_valid.orderBy('row_id').toJSON().collect()
fix_wkts

# COMMAND ----------

# MAGIC %md __Row 1: Fixed [Self-Intersection]__ 
# MAGIC
# MAGIC > Using GeoPandas to plot area for fixed.

# COMMAND ----------

gpd.GeoSeries(shapely.wkt.loads(json.loads(fix_wkts[0])['geom_wkt'])).plot()

# COMMAND ----------

# MAGIC %md __Row 2: Fixed [Self-Intersection]__
# MAGIC
# MAGIC > Using GeoPandas to plot area for fixed.

# COMMAND ----------

gpd.GeoSeries(shapely.wkt.loads(json.loads(fix_wkts[1])['geom_wkt'])).plot()

# COMMAND ----------

# MAGIC %md __Row 3: Fixed [Ring Self-Intersection]__
# MAGIC
# MAGIC > Using GeoPandas to plot area for fixed.

# COMMAND ----------

gpd.GeoSeries(shapely.wkt.loads(json.loads(fix_wkts[2])['geom_wkt'])).plot()

# COMMAND ----------

# MAGIC %md ## Option: Vectorized Pandas UDF
# MAGIC
# MAGIC > If you want to go further with performance, you can use a vectorized pandas UDF
# MAGIC
# MAGIC __Note: We are using the Pandas Series [Vectorized UDF](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.pandas_udf.html) variant.__

# COMMAND ----------

from pyspark.sql.functions import pandas_udf

@pandas_udf(StringType())
def vectorized_make_wkt_valid(s:pd.Series) -> pd.Series:
    """
    - test for wkt being valid
    - attempts to make valid
    - may have to change type, e.g. POLYGON to MULTIPOLYGON
     returns valid wkt
    """
    from shapely import wkt 
    from shapely.validation import make_valid

    def to_valid(w:str) -> str:
      _geom = wkt.loads(w)
      if _geom.is_valid:
        return w
      _geom_fix = make_valid(_geom)
      return _geom_fix.wkt

    return s.apply(to_valid) 

# COMMAND ----------

# MAGIC %md _This variation doesn't show all the interim testing, just the fixing._

# COMMAND ----------

df_valid1 = (
  df                                                                                   # <- initial dataframe
    .withColumnRenamed("geom_wkt", "orig_geom_wkt")
    .withColumn("is_orig_valid", mos.st_isvalid("orig_geom_wkt"))
    .repartition(sc.defaultParallelism * 8, "orig_geom_wkt")                           # <- useful at scale
  .select(
    "*",
    F
      .when(col("is_orig_valid") == False, vectorized_make_wkt_valid("orig_geom_wkt")) # <- Pandas UDF
      .otherwise(col("orig_geom_wkt"))
      .alias("geom_wkt")
  )
  .withColumn("is_valid", mos.st_isvalid("geom_wkt"))
  .drop("orig_geom_wkt")
)

print(f"""count? {df_valid1.count():,}""")
print(f"""num orig invalid? {df_valid1.filter(col("is_orig_valid") == False).count():,}""")
print(f"""num final invalid? {df_valid1.filter(col("is_valid") == False).count():,}""")
display(df_valid1)

# COMMAND ----------

# MAGIC %md > _To further optimize as an automated workflow, you would writing to Delta Tables and avoiding unnecessary calls to `count` / `display`._
# MAGIC
# MAGIC __Notes:__
# MAGIC
# MAGIC * At-scale, there are benefits to adding call like `.repartition(sc.defaultParallelism * 8, "orig_geom_wkt")` when coupled with spark confs to adjust AQE (see top of notebook) as this give you more control of partitioning since there is compute-heavy (aka UDF) tasks that Spark cannot plan for as well as a "pure" data-heavy operation.
# MAGIC * The focus of this notebook was not on rendering on a map, so we just used matplot lib with both Shapely (for pre-fixed geoms) and GeoPandas (for fixed geoms)
# MAGIC * The use of `.when()` conditional allows us to avoid UDF calls except where `is_valid=False` which saves on unnecessary compute time
# MAGIC * We avoided shapely `explain_validity` call except to initially understand as that call can be computationally expensive (and is only informational)
# MAGIC * This is just a subset of validation, but hopefully offers enough breadcrumbs for common issues you may face when processing invalid geometries
