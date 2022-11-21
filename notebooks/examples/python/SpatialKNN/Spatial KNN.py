# Databricks notebook source
# MAGIC %md
# MAGIC # Scalable KNN on Databricks with Mosaic

# COMMAND ----------

# MAGIC %python
# MAGIC slide_id = '1zVQFIVpjZ3A5jFswj43R7eAm2mxaWtxv75n-PWXwge4'
# MAGIC slide_number = '6'
# MAGIC 
# MAGIC displayHTML(f"""
# MAGIC 
# MAGIC <iframe
# MAGIC   src="https://docs.google.com/presentation/d/{slide_id}/embed?slide={slide_number}&rm=minimal"
# MAGIC   frameborder="0"
# MAGIC   width="100%"
# MAGIC   height="700"
# MAGIC ></iframe>
# MAGIC """)

# COMMAND ----------

# MAGIC %python
# MAGIC slide_id = '1zVQFIVpjZ3A5jFswj43R7eAm2mxaWtxv75n-PWXwge4'
# MAGIC slide_number = '8'
# MAGIC 
# MAGIC displayHTML(f"""
# MAGIC 
# MAGIC <iframe
# MAGIC   src="https://docs.google.com/presentation/d/{slide_id}/embed?slide={slide_number}&rm=minimal"
# MAGIC   frameborder="0"
# MAGIC   width="100%"
# MAGIC   height="700"
# MAGIC ></iframe>
# MAGIC """)

# COMMAND ----------

# MAGIC %python
# MAGIC slide_id = '19bkS6S0zwuWHh_NtY1g6QSr7qZ6RPRLqDMd1smMwmE0'
# MAGIC slide_number = '31'
# MAGIC 
# MAGIC displayHTML(f"""
# MAGIC 
# MAGIC <iframe
# MAGIC   src="https://docs.google.com/presentation/d/{slide_id}/embed?slide={slide_number}&rm=minimal"
# MAGIC   frameborder="0"
# MAGIC   width="100%"
# MAGIC   height="700"
# MAGIC ></iframe>
# MAGIC """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install mosaic and enable it

# COMMAND ----------

# MAGIC %pip install databricks-mosaic --quiet

# COMMAND ----------

from pyspark.sql import functions as F
import mosaic as mos

spark.conf.set("spark.databricks.optimizer.adaptive.enabled", "false")
from mosaic import enable_mosaic

enable_mosaic(spark, dbutils)

# COMMAND ----------

# MAGIC %md
# MAGIC # Load data
# MAGIC We will load a handfull of datasets we have prepared in our data prep notebook. <br>
# MAGIC For this use case we will first manually walk through the approach and then we will apply the model that comes with mosaic.

# COMMAND ----------

subway_stations = spark.read.table("sdsc_database.subway_stations")
streets = spark.read.table("sdsc_database.streets")
buildings = spark.read.table("sdsc_database.buildings").where(mos.st_geometrytype(F.col("geometry")) == "Point")
buildings_shapes = spark.read.table("sdsc_database.buildings").where(mos.st_geometrytype(F.col("geometry")) == "MultiPolygon")
trips = spark.read.table("sdsc_database.taxi_trips")

# COMMAND ----------

# MAGIC %md
# MAGIC We will visualise our building shapes and krings and kloops around the shapes. <br>

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC buildings_shapes "geometry" "geometry" 500

# COMMAND ----------

# MAGIC %md
# MAGIC In order to find out the nearest neigbours we can create a kring around each of our point of interests. <br>
# MAGIC For that purpose mosaic comes with geometry concious kring and kdisc (hexring) implementations. <br>
# MAGIC These expressions also have their auto-explode versions that we are going to use here. <br>
# MAGIC It is much easier to join already exploded cell IDs between 2 datasets.

# COMMAND ----------

with_kring_1 = buildings_shapes.select(
  F.col("geometry"),
  mos.grid_geometrykringexplode("geometry", F.lit(9), F.lit(1))
)

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC with_kring_1 "cellId" "h3" 500

# COMMAND ----------

# MAGIC %md
# MAGIC But what do we do if we dont have enough neighbours in the krings we just ran? </br>
# MAGIC We need to keep iterating. Our second iteration and all iterations onward are kdisc based. <br>
# MAGIC This allows us to only compare candidates we absolutely need to compare.

# COMMAND ----------

with_kdisc_2 = buildings_shapes.select(
  F.col("geometry"),
  mos.grid_geometrykloopexplode("geometry", F.lit(9), F.lit(2))
)

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC with_kdisc_2 "cellId" "h3"

# COMMAND ----------

# MAGIC %md
# MAGIC This is great, but what about complex shapes that are do not require radial catchment areas? <br>
# MAGIC What about data like streets or rivers? <br>
# MAGIC Mosaic's implementation of geometry concious krings and kdiscs can be used here.

# COMMAND ----------

with_kdisc_3 = streets.select(
  F.col("geometry"),
  mos.grid_geometryloopexplode("geometry", F.lit(9), F.lit(2))
)

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC with_kdisc_3 "cellId" "h3"

# COMMAND ----------

# MAGIC %md
# MAGIC There are a lot of things to keep track of if one is to implemet a scalable KNN approach. <br>
# MAGIC Luckily Mosaic comes with an implemetation of a spark transformer that can do all of those steps for us. <br>

# COMMAND ----------

from mosaic.models import ApproximateSpatialKNN
import mlflow

mlflow.autolog(disable=False)
 
with mlflow.start_run():
  candidates = trips.where("pickup_point is not null") 

  knn = ApproximateSpatialKNN()
  knn.setCandidatesDf(candidates)
  knn.setCandidatesFeatureCol("pickup_point")
  knn.setLandmarksFeatureCol("geometry")
  knn.setKNeighbours(20)
  knn.setMaxIterations(10)
  knn.setEarlyStopping(3)
  knn.setDistanceThreshold(1.0)
  knn.setCheckpointTablePrefix("checkpoint_table_knn")
  knn.setIndexResolution(10)

  knn.model.cleanupCheckpoint()
  
  neighbours = knn.transform(buildings_shapes)

# COMMAND ----------

neighbours = knn.transform(buildings_shapes)

# COMMAND ----------

neighbours.display()

# COMMAND ----------

trips.count()

# COMMAND ----------

buildings_shapes.count()

# COMMAND ----------

# MAGIC %md
# MAGIC In this example we have compared ~50K building shapes (polygons) to 1.6M taxi trips (points). <br>
# MAGIC This operation took slightly less than 5 minutes on the default cluster size with no extra tuning. <br>
# MAGIC Since this approach is defined as an algorithm it can beeasily chanined. <br>
# MAGIC E.g. We could now check using another instance of the knn model which streets are closest to the set of taxi trips that are idetified in the first run.

# COMMAND ----------

# MAGIC %md
# MAGIC Finally we can visualise our neighbour sets in kepler and verify that results make sense.

# COMMAND ----------

knn_geoms = neighbours.select("geometry", "pickup_point", "dropoff_point")

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC knn_geoms "geometry" "geometry" 

# COMMAND ----------


