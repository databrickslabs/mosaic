-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Setup NYC taxi zones
-- MAGIC In order to setup the data please run the notebook available at "../../data/DownloadNYCTaxiZones". </br>
-- MAGIC DownloadNYCTaxiZones notebook will make sure we have New York City Taxi zone shapes available in our environment.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
-- MAGIC 
-- MAGIC raw_path = f"dbfs:/tmp/mosaic/{user_name}"
-- MAGIC raw_taxi_zones_path = f"{raw_path}/taxi_zones"
-- MAGIC 
-- MAGIC print(f"The raw data is stored in {raw_path}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Enable Mosaic in the notebook
-- MAGIC To get started, you'll need to attach the JAR to your cluster and import instances as in the cell below. </br>

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import mosaic as mos
-- MAGIC mos.enable_mosaic(spark, dbutils)

-- COMMAND ----------

-- MAGIC %md ## Read polygons from GeoJson

-- COMMAND ----------

-- MAGIC %md
-- MAGIC With the functionallity Mosaic brings we can easily load GeoJSON files using spark. </br>
-- MAGIC In the past this required GeoPandas in python and conversion to spark dataframe. </br>
-- MAGIC Scala and SQL were hard to demo. </br>

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Note: Here we are using python as a proxy to programmatically
-- MAGIC # pass the location of our data source for taxi zones.
-- MAGIC spark.sql(f"drop table if exists taxi_zones;")
-- MAGIC spark.sql(
-- MAGIC   f"""
-- MAGIC      create table if not exists taxi_zones
-- MAGIC      using json
-- MAGIC      options (multiline = true)
-- MAGIC      location "{raw_taxi_zones_path}";
-- MAGIC      """
-- MAGIC )

-- COMMAND ----------

create or replace temp view neighbourhoods as (
  select
    type,
    feature.properties as properties,
    st_astext(st_geomfromgeojson(to_json(feature.geometry))) as geometry 
  from (
    select
      type,
      explode(features) as feature
    from taxi_zones
  )
);
select * from neighbourhoods;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##  Compute some basic geometry attributes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Mosaic provides a number of functions for extracting the properties of geometries. Here are some that are relevant to Polygon geometries:

-- COMMAND ----------

select geometry, st_area(geometry), st_length(geometry) from neighbourhoods;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Read points data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We will load some Taxi trips data to represent point data. </br>
-- MAGIC We already loaded some shapes representing polygons that correspond to NYC neighbourhoods. </br>

-- COMMAND ----------

create table if not exists nyctaxi_yellow 
using delta
location "/databricks-datasets/nyctaxi/tables/nyctaxi_yellow"

-- COMMAND ----------

create or replace temp view trips as (
  select 
    trip_distance,
    pickup_datetime,
    dropoff_datetime,
    st_astext(st_point(pickup_longitude, pickup_latitude)) as pickup_geom,
    st_astext(st_point(dropoff_longitude, dropoff_latitude)) as dropoff_geom,
    total_amount
  from nyctaxi_yellow
)

-- COMMAND ----------

select * from trips

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Spatial Joins

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can use Mosaic to perform spatial joins both with and without Mosaic indexing strategies. </br>
-- MAGIC Indexing is very important when handling very different geometries both in size and in shape (ie. number of vertices). </br>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Getting the optimal resolution

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can use Mosaic functionality to identify how to best index our data based on the data inside the specific dataframe. </br>
-- MAGIC Selecting an apropriate indexing resolution can have a considerable impact on the performance. </br>

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from mosaic import MosaicFrame
-- MAGIC 
-- MAGIC neighbourhoods_mosaic_frame = MosaicFrame(spark.read.table("neighbourhoods"), "geometry")
-- MAGIC optimal_resolution = neighbourhoods_mosaic_frame.get_optimal_resolution(sample_fraction=0.75)
-- MAGIC 
-- MAGIC print(f"Optimal resolution is {optimal_resolution}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC It is worth noting that not each resolution will yield performance improvements. </br>
-- MAGIC By a rule of thumb it is always better to under index than over index - if not sure select a lower resolution. </br>
-- MAGIC Higher resolutions are needed when we have very imbalanced geometries with respect to their size or with respect to the number of vertices. </br>
-- MAGIC In such case indexing with more indices will considerably increase the parallel nature of the operations. </br>
-- MAGIC You can think of Mosaic as a way to partition an overly complex row into multiple rows that have a balanced amount of computation each.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC   neighbourhoods_mosaic_frame.get_resolution_metrics(sample_rows=150)
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Indexing using the optimal resolution

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We will use mosaic sql functions to index our points data. </br>
-- MAGIC Here we will use resolution 9, index resolution depends on the dataset in use.

-- COMMAND ----------

create or replace temp view tripsWithIndex as (
  select 
    *,
    grid_pointascellid(pickup_geom, 9) as pickup_h3,
    grid_pointascellid(dropoff_geom, 9) as dropoff_h3,
    st_makeline(array(pickup_geom, dropoff_geom)) as trip_line
  from trips
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We will also index our neighbourhoods using a built in generator function.

-- COMMAND ----------

create or replace temp view neighbourhoodsWithIndex as (
  select 
    *,
    grid_tessellateexplode(geometry, 9) as mosaic_index
  from neighbourhoods
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Performing the spatial join

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can now do spatial joins to both pickup and drop off zones based on geolocations in our datasets.

-- COMMAND ----------

create or replace temp view withPickupZone as (
  select 
     trip_distance, pickup_geom, dropoff_geom, pickup_h3, dropoff_h3, pickup_zone, trip_line
  from tripsWithIndex
  join (
    select 
      properties.zone as pickup_zone,
      mosaic_index
    from neighbourhoodsWithIndex
  )
  on mosaic_index.index_id == pickup_h3
  where mosaic_index.is_core or st_contains(mosaic_index.wkb, pickup_geom)
);
select * from withPickupZone;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can easily perform a similar join for the drop off location.

-- COMMAND ----------

create or replace temp view withDropoffZone as (
  select 
     trip_distance, pickup_geom, dropoff_geom, pickup_h3, dropoff_h3, pickup_zone, dropoff_zone, trip_line
  from withPickupZone
  join (
    select 
      properties.zone as dropoff_zone,
      mosaic_index
    from neighbourhoodsWithIndex
  )
  on mosaic_index.index_id == dropoff_h3
  where mosaic_index.is_core or st_contains(mosaic_index.wkb, dropoff_geom)
);
select * from withDropoffZone;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Visualise the results in Kepler

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For visualisation there simply aren't good options in scala. </br>
-- MAGIC Luckily in our notebooks you can easily switch to python just for UI. </br>
-- MAGIC Mosaic abstracts interaction with Kepler in python.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %%mosaic_kepler
-- MAGIC "withDropoffZone" "pickup_h3" "h3" 5000 

-- COMMAND ----------


