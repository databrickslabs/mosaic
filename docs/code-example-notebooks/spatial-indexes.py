# Databricks notebook source
# MAGIC %md
# MAGIC # Using grid index systems in Mosaic

# COMMAND ----------

from pyspark.sql.functions import *
from mosaic import enable_mosaic
enable_mosaic(spark, dbutils)

# COMMAND ----------

# MAGIC %md
# MAGIC Set operations over big geospatial datasets become very expensive without some form of spatial indexing.
# MAGIC 
# MAGIC Spatial indexes not only allow operations like point-in-polygon joins to be partitioned but, if only approximate results are required, can be used to reduce these to deterministic SQL joins directly on the indexes.

# COMMAND ----------

# MAGIC %md
# MAGIC ![example h3 point-in-poly image](https://databricks.com/wp-content/uploads/2021/01/blog-geospatial-3.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC The workflow for a point-in-poly spatial join might look like the following:

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read the source point and polygon datasets.

# COMMAND ----------

drop_cols = [
  "rate_code_id", "store_and_fwd_flag", "dropoff_longitude",
  "dropoff_latitude", "payment_type", "fare_amount",
  "extra", "mta_tax", "tip_amount", "tolls_amount",
  "total_amount"
]

trips = (
  spark.table("delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow`")
  .drop(*drop_cols)
  .limit(5_000_000)
  .repartition(sc.defaultParallelism * 20)
)

trips.show()

# COMMAND ----------

from mosaic import st_geomfromgeojson

user = spark.sql("select current_user() as user").collect()[0]["user"]

neighbourhoods = (
  spark.read.format("json")
  .load(f"dbfs:/FileStore/shared_uploads/{user}/NYC_Taxi_Zones.geojson")
  .repartition(sc.defaultParallelism)
  .withColumn("geometry", st_geomfromgeojson(to_json(col("geometry"))))
  .select("properties.*", "geometry")
  .drop("shape_area", "shape_leng")
)

neighbourhoods.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Compute the resolution of index required to optimize the join.

# COMMAND ----------

from mosaic import MosaicFrame

neighbourhoods_mdf = MosaicFrame(neighbourhoods, "geometry")
help(neighbourhoods_mdf.get_optimal_resolution)

# COMMAND ----------

(resolution := neighbourhoods_mdf.get_optimal_resolution(sample_fraction=1.))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Apply the index to the set of points in your left-hand dataframe.
# MAGIC This will generate an index value that corresponds to the grid ‘cell’ that this point occupies.

# COMMAND ----------

from mosaic import grid_longlatascellid
indexed_trips = trips.withColumn("ix", grid_longlatascellid(lon="pickup_longitude", lat="pickup_latitude", resolution=lit(resolution)))
indexed_trips.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Compute the set of indices that fully covers each polygon in the right-hand dataframe
# MAGIC This is commonly referred to as a polyfill operation.

# COMMAND ----------

from mosaic import grid_polyfill

indexed_neighbourhoods = (
  neighbourhoods
  .select("*", grid_polyfill("geometry", lit(resolution)).alias("ix_set"))
  .drop("geometry")
)

indexed_neighbourhoods.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. ‘Explode’ the polygon index dataframe, such that each polygon index becomes a row in a new dataframe.

# COMMAND ----------

exploded_indexed_neighbourhoods = (
  indexed_neighbourhoods
  .withColumn("ix", explode("ix_set"))
  .drop("ix_set")
)

exploded_indexed_neighbourhoods.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Join the new left- and right-hand dataframes directly on the index.

# COMMAND ----------

joined_df = (
  indexed_trips.alias("t")
  .join(exploded_indexed_neighbourhoods.alias("n"), on="ix", how="inner"))
joined_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final notes
# MAGIC Mosaic provides support for Uber’s H3 spatial indexing library as a core part of the API, but we plan to add support for other index systems, including S2 and British National Grid in due course.
