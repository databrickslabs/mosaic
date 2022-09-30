// Databricks notebook source
// MAGIC %md
// MAGIC ## Setup NYC taxi zones
// MAGIC In order to setup the data please run the notebook available at "../../data/DownloadNYCTaxiZones". </br>
// MAGIC DownloadNYCTaxiZones notebook will make sure we have New York City Taxi zone shapes available in our environment.

// COMMAND ----------

val user_name = dbutils.notebook.getContext.userName.get

val raw_path = s"dbfs:/tmp/mosaic/$user_name"
val raw_taxi_zones_path = s"$raw_path/taxi_zones"

print(s"The raw data is stored in $raw_path")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Enable Mosaic in the notebook
// MAGIC Mosaic requires Databricks Runtime (DBR) version 10.0 or higher (11.2 with photon or higher is recommended). </br>
// MAGIC To get started, you'll need to attach the JAR to your cluster and import instances as in the cell below.

// COMMAND ----------

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.H3
import com.databricks.labs.mosaic.ESRI

val mosaicContext = MosaicContext.build(H3, ESRI)
import mosaicContext.functions._
import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md ## Read polygons from GeoJson

// COMMAND ----------

// MAGIC %md
// MAGIC With the functionality Mosaic brings we can easily load GeoJSON files using spark. </br>
// MAGIC In the past this required GeoPandas in python and conversion to spark dataframe. </br>

// COMMAND ----------

val neighbourhoods = (
  spark.read
    .option("multiline", "true")
    .format("json")
    .load(raw_taxi_zones_path)
    .select(col("type"), explode(col("features")).alias("feature"))
    .select(col("type"), col("feature.properties").alias("properties"), to_json(col("feature.geometry")).alias("json_geometry"))
    .withColumn("geometry", st_aswkt(st_geomfromgeojson(col("json_geometry"))))
)

display(
  neighbourhoods
)

// COMMAND ----------

// MAGIC %md
// MAGIC ##  Compute some basic geometry attributes

// COMMAND ----------

// MAGIC %md
// MAGIC Mosaic provides a number of functions for extracting the properties of geometries. Here are some that are relevant to Polygon geometries:

// COMMAND ----------

display(
  neighbourhoods
    .withColumn("calculatedArea", st_area(col("geometry")))
    .withColumn("calculatedLength", st_length(col("geometry")))
    // Note: The unit of measure of the area and length depends on the CRS used.
    // For GPS locations it will be square radians and radians
    .select("geometry", "calculatedArea", "calculatedLength")
)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Read points data

// COMMAND ----------

// MAGIC %md
// MAGIC We will load some Taxi trips data to represent point data. </br>
// MAGIC We already loaded some shapes representing polygons that correspond to NYC neighbourhoods. </br>

// COMMAND ----------

val tripsTable = spark.table("delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow`")

// COMMAND ----------

val trips = tripsTable
  .drop("vendorId", "rateCodeId", "store_and_fwd_flag", "payment_type")
  .withColumn("pickup_geom", st_astext(st_point(col("pickup_longitude"), col("pickup_latitude"))))
  .withColumn("dropoff_geom", st_astext(st_point(col("dropoff_longitude"), col("dropoff_latitude"))))

// COMMAND ----------

display(trips.select("pickup_geom", "dropoff_geom"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Spatial Joins

// COMMAND ----------

// MAGIC %md
// MAGIC We can use Mosaic to perform spatial joins both with and without Mosaic indexing strategies. </br>
// MAGIC Indexing is very important when handling very different geometries both in size and in shape (ie. number of vertices). </br>

// COMMAND ----------

// MAGIC %md
// MAGIC ### Getting the optimal resolution

// COMMAND ----------

// MAGIC %md
// MAGIC We can use Mosaic functionality to identify how to best index our data based on the data inside the specific dataframe. </br>
// MAGIC Selecting an apropriate indexing resolution can have a considerable impact on the performance. </br>

// COMMAND ----------

import com.databricks.labs.mosaic.sql.MosaicFrame

val mosaicFrame = MosaicFrame(neighbourhoods)
  .setGeometryColumn("geometry")

val optimalResolution = mosaicFrame.getOptimalResolution(0.75)

println(s"Optimal resolution is $optimalResolution")

// COMMAND ----------

// MAGIC %md
// MAGIC Not every resolution will yield performance improvements. </br>
// MAGIC By a rule of thumb it is always better to under-index than over-index - if not sure select a lower resolution. </br>
// MAGIC Higher resolutions are needed when we have very imbalanced geometries with respect to their size or with respect to the number of vertices. </br>
// MAGIC In such case indexing with more indices will considerably increase the parallel nature of the operations. </br>
// MAGIC You can think of Mosaic as a way to partition an overly complex row into multiple rows that have a balanced amount of computation each.

// COMMAND ----------

display(
  mosaicFrame.analyzer.getResolutionMetrics()
)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Indexing using the optimal resolution

// COMMAND ----------

// MAGIC %md
// MAGIC We will use mosaic sql functions to index our points data. </br>
// MAGIC Here we will use resolution 9, index resolution depends on the dataset in use.

// COMMAND ----------

val tripsWithIndex = trips
  .withColumn("pickup_h3", grid_pointascellid(col("pickup_geom"), lit(optimalResolution)))
  .withColumn("dropoff_h3", grid_pointascellid(col("dropoff_geom"), lit(optimalResolution)))

display(tripsWithIndex)

// COMMAND ----------

// MAGIC %md
// MAGIC We will also index our neighbourhoods using a built in generator function.

// COMMAND ----------

val neighbourhoodsWithIndex = neighbourhoods
   // We break down the original geometry in multiple smaller mosaic chips, each with its
   // own index
   .withColumn("mosaic_index", grid_tessellateexplode(col("geometry"), lit(optimalResolution)))
   // We don't need the original geometry any more, since we have broken it down into
   // Smaller mosaic chips.
   .drop("json_geometry", "geometry")

display(neighbourhoodsWithIndex)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Performing the spatial join

// COMMAND ----------

// MAGIC %md
// MAGIC We can now do spatial joins to both pickup and drop off zones based on geolocations in our datasets.

// COMMAND ----------

val pickupNeighbourhoods = neighbourhoodsWithIndex.select(col("properties.zone").alias("pickup_zone"), col("mosaic_index"))

val withPickupZone = 
  tripsWithIndex.join(
    pickupNeighbourhoods,
    tripsWithIndex.col("pickup_h3") === pickupNeighbourhoods.col("mosaic_index.index_id")
  ).where(
    // If the borough is a core chip (the chip is fully contained within the geometry), then we do not need
    // to perform any intersection, because any point matching the same index will certainly be contained in
    // the borough. Otherwise we need to perform an st_contains operation on the chip geometry.
    col("mosaic_index.is_core") || st_contains(col("mosaic_index.wkb"), col("pickup_geom"))
  ).select(
    "trip_distance", "pickup_geom", "pickup_zone", "dropoff_geom", "pickup_h3", "dropoff_h3"
  )

display(withPickupZone)

// COMMAND ----------

// MAGIC %md
// MAGIC We can easily perform a similar join for the drop off location.

// COMMAND ----------

val dropoffNeighbourhoods = neighbourhoodsWithIndex.select(col("properties.zone").alias("dropoff_zone"), col("mosaic_index"))

val withDropoffZone = 
  withPickupZone.join(
    dropoffNeighbourhoods,
    withPickupZone.col("dropoff_h3") === dropoffNeighbourhoods.col("mosaic_index.index_id")
  ).where(
    col("mosaic_index.is_core") || st_contains(col("mosaic_index.wkb"), col("dropoff_geom"))
  ).select(
    "trip_distance", "pickup_geom", "pickup_zone", "dropoff_geom", "dropoff_zone", "pickup_h3", "dropoff_h3"
  )
  .withColumn("trip_line", st_astext(st_makeline(array(st_geomfromwkt(col("pickup_geom")), st_geomfromwkt(col("dropoff_geom"))))))

display(withDropoffZone)

// COMMAND ----------

withDropoffZone.createOrReplaceTempView("withDropoffZone")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Visualise the results in Kepler

// COMMAND ----------

// MAGIC %md
// MAGIC For visualisation there simply aren't good options in scala. </br>
// MAGIC Luckily in our notebooks you can easily switch to python just for UI. </br>
// MAGIC Mosaic abstracts interaction with Kepler in python.

// COMMAND ----------

// MAGIC %python
// MAGIC from mosaic import enable_mosaic
// MAGIC enable_mosaic(spark, dbutils)

// COMMAND ----------

// MAGIC %python
// MAGIC %%mosaic_kepler
// MAGIC "withDropoffZone" "pickup_h3" "h3" 5000
