# Databricks notebook source
# MAGIC %md
# MAGIC # Scalable KNN on Databricks with Mosaic
# MAGIC
# MAGIC > See [Blog](https://medium.com/@milos.colic/scalable-spatial-nearest-neighbours-with-mosaic-336ce37edbae) | [Mosaic Docs](https://databrickslabs.github.io/mosaic/models/spatial-knn.html) | [SpatialKNN API](https://github.com/databrickslabs/mosaic/blob/main/python/mosaic/models/knn/spatial_knn.py) -- __Note:__ Make sure you run this on Databricks ML Runtime.
# MAGIC
# MAGIC <p/>
# MAGIC
# MAGIC 1. To use Databricks Labs [Mosaic](https://databrickslabs.github.io/mosaic/index.html) library for geospatial data engineering, analysis, and visualization functionality:
# MAGIC   * Install with `%pip install databricks-mosaic`
# MAGIC   * Import and use with the following:
# MAGIC   ```
# MAGIC   import mosaic as mos
# MAGIC   mos.enable_mosaic(spark, dbutils)
# MAGIC   ```
# MAGIC <p/>
# MAGIC
# MAGIC 2. To use [KeplerGl](https://kepler.gl/) OSS library for map layer rendering:
# MAGIC   * Already installed with Mosaic, use `%%mosaic_kepler` magic [[Mosaic Docs](https://databrickslabs.github.io/mosaic/usage/kepler.html)]
# MAGIC   * Import with `from keplergl import KeplerGl` to use directly
# MAGIC
# MAGIC If you have trouble with Volume access:
# MAGIC
# MAGIC * For Mosaic 0.3 series (< DBR 13)     - you can copy resources to DBFS as a workaround
# MAGIC * For Mosaic 0.4 series (DBR 13.3 LTS) - you will need to either copy resources to DBFS or setup for Unity Catalog + Shared Access which will involve your workspace admin. Instructions, as updated, will be [here](https://databrickslabs.github.io/mosaic/usage/install-gdal.html).
# MAGIC
# MAGIC ---
# MAGIC __Last Updated:__ 27 NOV 2023 [Mosaic 0.3.12]

# COMMAND ----------

# MAGIC %md
# MAGIC > Usually when asserting the notion of nearest neighbors we bound that notion to the _K_ neighbors, if left unbound the answers produced by the analysis are basically orderings of the whole data assets based on the proximity/distance and the computational costs to produce such outputs can be very prohibitive since they would result in comparing all features across all data assets.
# MAGIC
# MAGIC __Optimized Algorithm (Right Side Below)__
# MAGIC </p>  
# MAGIC
# MAGIC 1. For each geometry in set L generate a kloop (hollow ring)
# MAGIC 1. Generate match candidates within 
# MAGIC 1. For each match candidate C calculate the distance to the landmark
# MAGIC 1. For each L[i] count the matches; stop if count =  k 
# MAGIC 1. If count < k, increase the size of the kloop;  repeat (s1)
# MAGIC 1. If count > k, remove matches furthest from the L[i]; stop
# MAGIC 1. Optional: early stopping if no new match candidates are found in the kloop of any L geometry for N iterations 
# MAGIC 1. Continue with the next kloop up to max iterations
# MAGIC 1. Return C geometries with smallest distance to each L[i]

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC displayHTML(f"""
# MAGIC <img
# MAGIC   src="https://miro.medium.com/v2/resize:fit:1400/0*DEuwg-aDj_maPVX0"
# MAGIC   width="50%"
# MAGIC ></img>
# MAGIC """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install + Enable Mosaic

# COMMAND ----------

# MAGIC %pip install "databricks-mosaic<0.4,>=0.3" --quiet # <- Mosaic 0.3 series
# MAGIC # %pip install "databricks-mosaic<0.5,>=0.4" --quiet # <- Mosaic 0.4 series (as available)

# COMMAND ----------

# -- configure AQE for more compute heavy operations
#  - choose option-1 or option-2 below, essential for REPARTITION!
# spark.conf.set("spark.databricks.optimizer.adaptive.enabled", False) # <- option-1: turn off completely for full control
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False) # <- option-2: just tweak partition management
spark.conf.set("spark.sql.shuffle.partitions", 1_024)                  # <-- default is 200

# -- import databricks + spark functions
from pyspark.databricks.sql import functions as dbf
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import *

# -- setup mosaic
import mosaic as mos

mos.enable_mosaic(spark, dbutils)
# mos.enable_gdal(spark) # <- not needed for this example

# --other imports
import warnings

warnings.simplefilter("ignore")

# COMMAND ----------

user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
print(f"username? '{user_name}'")
      
spark.sparkContext.setCheckpointDir(f"dbfs:/tmp/mosaic/{user_name}/checkpoints")

# COMMAND ----------

# MAGIC %md __Setup Catalog and Schema__
# MAGIC
# MAGIC > These values will mirror the Data Prep notebook.

# COMMAND ----------

catalog_name = "mjohns"
db_name = "mosaic_spatial_knn"
sql(f"USE CATALOG {catalog_name}")
sql(f"USE SCHEMA {db_name}")

# COMMAND ----------

# MAGIC %sql show tables

# COMMAND ----------

# MAGIC %md ## Load Landmark + Candidates Tables
# MAGIC
# MAGIC > We will load a handfull of datasets we have prepared in our data prep notebook. For this use case we will first manually walk through the approach and then we will apply the model that comes with mosaic.

# COMMAND ----------

df_bldg = spark.read.table("building_50k").where(mos.st_geometrytype(F.col("geom_wkt")) == "POINT")
df_bldg_shape = spark.read.table("building_50k").where(mos.st_geometrytype(F.col("geom_wkt")) == "MULTIPOLYGON")
df_trip = spark.read.table("taxi_trip_1m")

# sanity checks on counts (may vary based on your sample)
print(f"Bldg POINT count? {df_bldg.count():,}")
print(f"Bldg MULTIPOLYGON count? {df_bldg_shape.count():,}")
print(f"Trip count? {df_trip.count():,}")

# COMMAND ----------

# MAGIC %md ## Render with Kepler
# MAGIC > We will render our building shapes and krings and kdiscs / kloops around the shapes; showing 1% subset of building, you can pan and zoom in the viewport.

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_bldg_shape "geom_wkt" "geometry" 500

# COMMAND ----------

# MAGIC %md
# MAGIC > In order to find out the nearest neighbors we can create a kring around each of our point of interests. For that purpose mosaic comes with geometry aware kring and kdisc / kloop (hexring) implementations. These expressions also have their auto-explode versions that we are going to use here. It is much easier to join already exploded cell IDs between 2 datasets. __Note: the gridding system is h3 [[1](https://docs.databricks.com/en/sql/language-manual/sql-ref-h3-geospatial-functions.html)|[2](https://h3geo.org/)] by default; this example uses resolution 9 and does just 1 kring around the geometry.__

# COMMAND ----------

with_kring_1 = df_bldg_shape.select(
  "geom_wkt",
  mos.grid_geometrykringexplode("geom_wkt", F.lit(9), F.lit(1))
)

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC with_kring_1 "cellId" "h3" 500

# COMMAND ----------

# MAGIC %md
# MAGIC > But what do we do if we dont have enough neighbors in the krings we just ran? We need to keep iterating. Our second iteration and all iterations onward are kdisc / kloop based. This allows us to only compare candidates we absolutely need to compare. __Note: example uses resolution=`9` again and kloop=`2`.__

# COMMAND ----------

with_kdisc_2 = df_bldg_shape.select(
  "geom_wkt",
  mos.grid_geometrykloopexplode("geom_wkt", F.lit(9), F.lit(2))
)

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC with_kdisc_2 "cellId" "h3"

# COMMAND ----------

# MAGIC %md
# MAGIC > This is great, but what about complex shapes that are do not require radial catchment areas? What about data like streets or rivers? Mosaic's implementation of geometry aware krings and kloops can be used here as well (not shown).
# MAGIC
# MAGIC ```
# MAGIC with_kdisc_3 = streets.select(
# MAGIC   F.col("geometry"),
# MAGIC   mos.grid_geometrykloopexplode("geometry", F.lit(9), F.lit(2))
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md ## Prep for KNN
# MAGIC
# MAGIC > There are a lot of things to keep track of if one is to implemet a scalable KNN approach. Luckily Mosaic comes with an implemetation of a spark transformer that can do all of those steps for us. __Note: The following requires [Databricks Runtime for Machine Learning](https://docs.databricks.com/en/release-notes/runtime/index.html).__

# COMMAND ----------

from mosaic.models import SpatialKNN
import mlflow

mlflow.autolog(disable=False)

# COMMAND ----------

# MAGIC %md __Look at Landmarks (`df_bldg_shape` | ~48K)__

# COMMAND ----------

print(f"landmarks (building shapes) count? {df_bldg_shape.count():,}")
df_bldg_shape.limit(3).display()

# COMMAND ----------

# MAGIC %md __Look at Candidates (`df_trip` | 1M)__

# COMMAND ----------

print(f"\tcandidates (trips) count? {df_trip.count():,}")
df_trip.limit(3).display()

# COMMAND ----------

# MAGIC %md ## Run the KNN Transform
# MAGIC
# MAGIC > In this example we will compare ~50K building shapes (polygons) to 1M taxi trips (points). Since this approach is defined as an algorithm it can be easily chained. E.g. We could, as a follow-on, check using another instance of the knn model which streets are closest to the set of taxi trips that are idetified in the first run (not shown).
# MAGIC
# MAGIC The transformer has the following parameters, from [here](https://databrickslabs.github.io/mosaic/models/spatial-knn.html):
# MAGIC
# MAGIC <p/>
# MAGIC
# MAGIC * `candidatesDf`: the dataframe containing the geometries that will be used as candidates for the KNN search
# MAGIC * `candidatesFeatureCol`: the name of the column that contains the candidates geometries
# MAGIC * `candidatesRowID`: the name of the column that contains the candidates ids
# MAGIC * `landmarksFeatureCol`: the name of the column that contains the landmarks geometries
# MAGIC * `landmarksRowID`: the name of the column that contains the landmarks ids
# MAGIC * `kNeighbours`: the number of neighbours to return
# MAGIC * `maxIterations`: the maximum number of iterations to perform
# MAGIC * `distanceThreshold`: the distance threshold to stop the iterations (in CRS units)
# MAGIC * `earlyStopIterations`: the number of subsequent iterations upon which to stop if no new neighbours
# MAGIC * `checkpointTablePrefix`: the prefix of the checkpoint table
# MAGIC * `indexResolution`: the resolution of the index (grid system specific)
# MAGIC * `approximate`: whether to stop after max iterations (approximate = true) or to perform the finalisation step (approximate = false) - no default value, the caller must specify this parameter

# COMMAND ----------

with mlflow.start_run():  

  knn = SpatialKNN()
  knn.setUseTableCheckpoint(True)
  knn.setCheckpointTablePrefix("checkpoint_table_knn")
  knn.model.cleanupCheckpoint
  
  knn.setApproximate(True)
  knn.setKNeighbours(20)
  knn.setIndexResolution(10)
  knn.setMaxIterations(10)
  knn.setEarlyStopIterations(3)
  knn.setDistanceThreshold(1.0)
  
  knn.setLandmarksFeatureCol("geom_wkt")
  knn.setLandmarksRowID("landmarks_id")
  
  knn.setCandidatesFeatureCol("pickup_point")
  knn.setCandidatesRowID("candidates_id")
  knn.setCandidatesDf(df_trip.where("pickup_point is not null"))

  df_neigh = knn.transform(df_bldg_shape)
  
  mlflow.log_params(knn.getParams())
  mlflow.log_metrics(knn.getMetrics())

# COMMAND ----------

# MAGIC %md _Here is an example of a generated (reproducible) experiment run._
# MAGIC
# MAGIC

# COMMAND ----------

displayHTML("""<img src='https://databrickslabs.github.io/mosaic/_images/knn_mlflow_params.png' width='25%'/>""")

# COMMAND ----------

# MAGIC %md __Generate KNN Transform Result Table `transform_result` (~620K)__
# MAGIC
# MAGIC > Write out the results from `df_neigh` to delta lake 

# COMMAND ----------

(
  df_neigh
    .write
      .format("delta")
      .mode("overwrite")
      .saveAsTable(f"transform_result")
)

# COMMAND ----------

df_result = spark.table(f"transform_result")
print(f"SpatialKNN transform count? {df_result.count():,}")
df_result.limit(5).display() # <- limiting for ipynb only

# COMMAND ----------

# MAGIC %md ## Render Transform Results
# MAGIC
# MAGIC > Finally we can render our knn sets (from `df_neigh`) in kepler and verify that results make sense.

# COMMAND ----------

knn_geoms = df_result.select("geom_wkt", "pickup_point", "dropoff_point")

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC knn_geoms "geom_wkt" "geometry" 
