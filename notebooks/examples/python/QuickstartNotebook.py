# Databricks notebook source
# MAGIC %md
# MAGIC ## Enable Mosaic in the notebook
# MAGIC To get started, you'll need to attach the JAR to your cluster and import instances as in the cell below.

# COMMAND ----------

from pyspark.sql.functions import *
from mosaic import enable_mosaic
enable_mosaic(spark, dbutils)
from mosaic import *

# COMMAND ----------

# MAGIC %md ## Read polygons from GeoJson

# COMMAND ----------

# MAGIC %md
# MAGIC With the functionallity Mosaic brings we can easily load GeoJSON files using spark. </br>
# MAGIC In the past this required GeoPandas in python and conversion to spark dataframe. </br>
# MAGIC Scala and SQL were hard to demo. </br>

# COMMAND ----------

neighbourhoods = (
  spark.read.format("json")
    .load("dbfs:/FileStore/shared_uploads/stuart.lynn@databricks.com/NYC_Taxi_Zones.geojson")
    .withColumn("geometry", st_astext(st_geomfromgeojson(to_json(col("geometry")))))
    .select("properties.*", "geometry")
    .drop("shape_area", "shape_leng")
)

display(
  neighbourhoods
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Compute some basic geometry attributes

# COMMAND ----------

# MAGIC %md
# MAGIC Mosaic provides a number of functions for extracting the properties of geometries. Here are some that are relevant to Polygon geometries:

# COMMAND ----------

display(
  neighbourhoods
    .withColumn("calculatedArea", st_area(col("geometry")))
    .withColumn("calculatedLength", st_length(col("geometry")))
    .select("geometry", "calculatedArea", "calculatedLength")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read points data

# COMMAND ----------

# MAGIC %md
# MAGIC We will load some Taxi trips data to represent point data. </br>
# MAGIC We already loaded some shapes representing polygons that correspond to NYC neighbourhoods. </br>

# COMMAND ----------

tripsTable = spark.table("delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow`")

# COMMAND ----------

trips = (
  tripsTable
    .drop("vendorId", "rateCodeId", "store_and_fwd_flag", "payment_type")
    .withColumn("pickup_geom", st_astext(st_point(col("pickup_longitude"), col("pickup_latitude"))))
    .withColumn("dropoff_geom", st_astext(st_point(col("dropoff_longitude"), col("dropoff_latitude"))))
)

# COMMAND ----------

display(trips.select("pickup_geom", "dropoff_geom"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spatial Joins

# COMMAND ----------

# MAGIC %md
# MAGIC We can use Mosaic to perform spatial joins both with and without Mosaic indexing strategies. </br>
# MAGIC Indexing is very important when handling very different geometries both in size and in shape (ie. number of vertices). </br>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting the optimal resolution

# COMMAND ----------

# MAGIC %md
# MAGIC We can use Mosaic functionality to identify how to best index our data based on the data inside the specific dataframe. </br>
# MAGIC Selecting an apropriate indexing resolution can have a considerable impact on the performance. </br>

# COMMAND ----------

from mosaic import MosaicFrame

mosaicFrame = MosaicFrame(neighbourhoods, "geometry")
mosaicFrame.get_optimal_resolution(0.75)

# COMMAND ----------

# MAGIC %md
# MAGIC It is worth noting that not each resolution will yield performance improvements. </br>
# MAGIC By a rule of thumb it is always better to under index than over index - if not sure select a lower resolution. </br>
# MAGIC Higher resolutions are needed when we have very imballanced geometries with respect to their size or with respect to the number of vertices. </br>
# MAGIC In such case indexing with more indices will considerably increase the parallel nature of the operations. </br>
# MAGIC You can think of Mosaic as a way to partition an overly complex row into multiple rows that have a balanced amount of computation each.

# COMMAND ----------

display(
  mosaicFrame.get_resolution_metrics(0.75)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Indexing using the optimal resolution

# COMMAND ----------

# MAGIC %md
# MAGIC We will use mosaic sql functions to index our points data. </br>
# MAGIC Here we will use resolution 9, index resolution depends on the dataset in use.

# COMMAND ----------

tripsWithIndex = (trips
  .withColumn("pickup_h3", point_index(col("pickup_geom"), lit(9)))
  .withColumn("dropoff_h3", point_index(col("dropoff_geom"), lit(9)))
)

# COMMAND ----------

# MAGIC %md
# MAGIC We will also index our neighbourhoods using a built in generator function.

# COMMAND ----------

neighbourhoodsWithIndex = neighbourhoods.withColumn("mosaic_index", mosaic_explode(col("geometry"), lit(9)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performing the spatial join

# COMMAND ----------

# MAGIC %md
# MAGIC We can now do spatial joins to both pickup and drop off zones based on geolocations in our datasets.

# COMMAND ----------

pickupNeighbourhoods = neighbourhoodsWithIndex.select(col("zone").alias("pickup_zone"), col("mosaic_index"))

withPickupZone = (
  tripsWithIndex.join(
    pickupNeighbourhoods,
    tripsWithIndex["pickup_h3"] == pickupNeighbourhoods["mosaic_index.index_id"]
  ).where(
    col("mosaic_index.is_core") | st_contains(col("mosaic_index.wkb"), col("pickup_geom"))
  ).select(
    "trip_distance", "pickup_geom", "pickup_zone", "dropoff_geom", "pickup_h3", "dropoff_h3"
  )
)

display(withPickupZone)

# COMMAND ----------

# MAGIC %md
# MAGIC We can easily perform a similar join for the drop off location.

# COMMAND ----------

dropoffNeighbourhoods = neighbourhoodsWithIndex.select(col("zone").alias("dropoff_zone"), col("mosaic_index"))

withDropoffZone = (
  withPickupZone.join(
    dropoffNeighbourhoods,
    withPickupZone["dropoff_h3"] == dropoffNeighbourhoods["mosaic_index.index_id"]
  ).where(
    col("mosaic_index.is_core") | st_contains(col("mosaic_index.wkb"), col("dropoff_geom"))
  ).select(
    "trip_distance", "pickup_geom", "pickup_zone", "dropoff_geom", "dropoff_zone", "pickup_h3", "dropoff_h3"
  )
  .withColumn("trip_line", st_astext(st_makeline(array(st_geomfromwkt(col("pickup_geom")), st_geomfromwkt(col("dropoff_geom"))))))
)

display(withDropoffZone)

# COMMAND ----------

withDropoffZone.createOrReplaceTempView("withDropoffZone")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualise the results in Kepler

# COMMAND ----------

# MAGIC %md
# MAGIC For visualisation there simply arent good options in scala. </br>
# MAGIC Luckily in our notebooks you can easily switch to python just for UI. </br>
# MAGIC Mosaic abstracts interaction with Kepler in python.

# COMMAND ----------

# MAGIC %python
# MAGIC %%mosaic_kepler
# MAGIC "withDropoffZone" "pickup_h3" "h3" 5000

# COMMAND ----------


