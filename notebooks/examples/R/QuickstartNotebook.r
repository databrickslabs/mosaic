# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup NYC taxi zones
# MAGIC In order to setup the data please run the notebook available at "../../data/DownloadNYCTaxiZones". </br>
# MAGIC DownloadNYCTaxiZones notebook will make sure we have New York City Taxi zone shapes available in our environment.

# COMMAND ----------

# MAGIC %run ../../data/DownloadNYCTaxiZones

# COMMAND ----------

user_name <- SparkR::collect(SparkR::sql("select current_user()"))
raw_path <- paste0("dbfs:/tmp/mosaic/", user_name)
raw_taxi_zones_path = paste0(raw_path,"/taxi_zones")

print(paste0("The raw data is stored in ", raw_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enable Mosaic in the notebook
# MAGIC To get started, you'll need to attach the wheel to your cluster and import instances as in the cell below.

# COMMAND ----------


library(tidyverse)
library(SparkR)
sparkr_mosaic_package_path = '/dbfs/Users/robert.whiffin@databricks.com/mosaic/sparkrMosaic_0_1_0_tar.gz'
install.packages(sparkr_mosaic_package_path, repos=NULL)
library(sparkrMosaic)
sparkrMosaic::enableMosaic()

# COMMAND ----------

# MAGIC %md ## Read polygons from GeoJson

# COMMAND ----------

# MAGIC %md
# MAGIC With the functionality Mosaic brings we can easily load GeoJSON files using spark. </br>
# MAGIC In the past this required GeoPandas in python and conversion to spark dataframe. </br>

# COMMAND ----------

neighbourhoods <- 
  SparkR::read.json(
    raw_taxi_zones_path
    ,multiLine=T
  ) %>% SparkR::select(
  SparkR::column("type")
    ,SparkR::alias(SparkR::explode(SparkR::column("features")), "feature")
  ) %>% 
  SparkR::select(
    "type"
    ,"feature.properties"
    ,"feature.geometry"
  ) %>%
  SparkR::withColumn(
  "json_geometry"
  ,SparkR::to_json(SparkR::column("geometry"))
  ) %>%
SparkR::withColumn(
  "geometry"
  , sparkrMosaic::st_aswkt(sparkrMosaic::st_geomfromgeojson(column("json_geometry")))
)


# COMMAND ----------

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
  neighbourhoods %>%
  withColumn(
    "calculatedArea", sparkrMosaic::st_area(column("geometry"))
  ) %>%
  withColumn(
    "calculatedLength", sparkrMosaic::st_length(column("geometry"))
  ) %>%
  SparkR::select("geometry", "calculatedArea", "calculatedLength")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read points data

# COMMAND ----------

# MAGIC %md
# MAGIC We will load some Taxi trips data to represent point data. </br>
# MAGIC We already loaded some shapes representing polygons that correspond to NYC neighbourhoods. </br>

# COMMAND ----------

tripsTable <- SparkR::read.df("/databricks-datasets/nyctaxi/tables/nyctaxi_yellow", source="delta")

# COMMAND ----------

trips <- tripsTable %>%
  SparkR::drop(c("vendorId", "rateCodeId", "store_and_fwd_flag", "payment_type")) %>%
  withColumn(
    "pickup_geom", st_astext(st_point(SparkR::column("pickup_longitude"), SparkR::column("pickup_latitude")))
  ) %>%
  withColumn(
    "dropoff_geom", st_astext(st_point(SparkR::column("dropoff_longitude"), SparkR::column("dropoff_latitude")))
  ) 

# COMMAND ----------

display(trips  %>% SparkR::select("pickup_geom", "dropoff_geom"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spatial Joins

# COMMAND ----------

# MAGIC %md
# MAGIC We can use Mosaic to perform spatial joins both with and without Mosaic indexing strategies. </br>
# MAGIC Indexing is very important when handling very different geometries both in size and in shape (ie. number of vertices). </br>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Indexing using the optimal resolution

# COMMAND ----------

# MAGIC %md
# MAGIC We will use mosaic sql functions to index our points data. </br>
# MAGIC Here we will use resolution 9, index resolution depends on the dataset in use.

# COMMAND ----------

optimal_resolution <- 9L
tripsWithIndex <- trips %>%
  withColumn("pickup_h3", grid_pointascellid(column("pickup_geom"), lit(optimal_resolution))) %>%
  withColumn("dropoff_h3", grid_pointascellid(column("dropoff_geom"), lit(optimal_resolution)))


# COMMAND ----------

display(tripsWithIndex)

# COMMAND ----------

# MAGIC %md
# MAGIC We will also index our neighbourhoods using a built in generator function.

# COMMAND ----------

neighbourhoodsWithIndex <- 
 neighbourhoods %>%
 # We break down the original geometry in multiple smaller mosaic chips, each with its
 # own index
 withColumn("mosaic_index", grid_tessellateexplode(column("geometry"), lit(optimal_resolution))) %>%
 # We don't need the original geometry any more, since we have broken it down into
 # Smaller mosaic chips.
 drop(c("json_geometry", "geometry"))

# COMMAND ----------

display(neighbourhoodsWithIndex)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performing the spatial join

# COMMAND ----------

# MAGIC %md
# MAGIC We can now do spatial joins to both pickup and drop off zones based on geolocations in our datasets.

# COMMAND ----------

pickupNeighbourhoods <- neighbourhoodsWithIndex %>% 
  SparkR::select(
    column("properties.zone") %>% alias("pickup_zone")
    , column("mosaic_index")
  )

withPickupZone <-
  tripsWithIndex %>% join(
    pickupNeighbourhoods,
    tripsWithIndex$pickup_h3 == pickupNeighbourhoods$mosaic_index.index_id
  ) %>% 
  where(
    # If the borough is a core chip (the chip is fully contained within the geometry), then we do not need
    # to perform any intersection, because any point matching the same index will certainly be contained in
    # the borough. Otherwise we need to perform an st_contains operation on the chip geometry.
    column("mosaic_index.is_core") | st_contains(column("mosaic_index.wkb"), column("pickup_geom"))
  ) %>% 
  SparkR::select(
      column("trip_distance")
    , column("pickup_geom")
    , column("pickup_zone")
    , column("dropoff_geom")
    , column("pickup_h3")
    , column("dropoff_h3")
  )


display(withPickupZone)

# COMMAND ----------

# MAGIC %md
# MAGIC We can easily perform a similar join for the drop off location.

# COMMAND ----------

dropoffNeighbourhoods <-
  neighbourhoodsWithIndex %>% 
   SparkR::select(
     column("properties.zone") %>% alias("dropoff_zone")
     , column("mosaic_index")
   )

withDropoffZone = 
  withPickupZone %>% 
  join(
    dropoffNeighbourhoods,
    withPickupZone$dropoff_h3 == dropoffNeighbourhoods$mosaic_index.index_id
  ) %>% 
  where(
    column("mosaic_index.is_core") | st_contains(column("mosaic_index.wkb"), column("dropoff_geom"))
  ) %>% 
  SparkR::select(
      column("trip_distance")
    , column("pickup_geom")
    , column("pickup_zone")
    , column("dropoff_geom")
    , column("pickup_h3")
    , column("dropoff_h3")
  ) %>%
  withColumn("trip_line", 
             st_astext(
               st_makeline(
                 create_array(
                   st_geomfromwkt(column("pickup_geom"))
                   , st_geomfromwkt(column("dropoff_geom"))
                 )
               )
             )
            )


display(withDropoffZone)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualise the results in Kepler

# COMMAND ----------

# MAGIC %md
# MAGIC For now, visualisation are most easily done through Kepler in python. </br>
# MAGIC Luckily in our notebooks you can easily switch to python just for UI. </br>
# MAGIC Mosaic abstracts interaction with Kepler in python.

# COMMAND ----------

# MAGIC %python
# MAGIC import mosaic as mos
# MAGIC mos.enable_mosaic(spark, dbutils)

# COMMAND ----------

# We are using a temp view to pass the dataframe from R to python
withDropoffZone %>% createOrReplaceTempView("withDropoffZone")

# COMMAND ----------

# MAGIC %python
# MAGIC %%mosaic_kepler
# MAGIC "withDropoffZone" "pickup_h3" "h3" 5000
