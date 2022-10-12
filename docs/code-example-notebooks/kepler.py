# Databricks notebook source
# MAGIC %md
# MAGIC # Kepler visualizations

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC You can use the `%%mosaic_kepler` magic function to visualise data using [Kepler.gl](https://kepler.gl/).
# MAGIC 
# MAGIC The mosaic_kepler magic function accepts four parameters:
# MAGIC 
# MAGIC 1) `dataset`: Can be a Spark dataset or a string representing a table/view name
# MAGIC 
# MAGIC 2) `column_name`: The column that needs to be plotted, can be either a geometry column (`WKT`, `WKB` or Mosaic internal format) or a column containing a spatial grid index ID
# MAGIC 
# MAGIC 3) `feature_type`: The type of data to be plotted. Valid values are `geometry` and `h3`
# MAGIC 
# MAGIC 4) `limit`: The maximum number of objects to plot. The default limit is `1000`
# MAGIC 
# MAGIC Usage:
# MAGIC ```
# MAGIC %%mosaic_kepler
# MAGIC dataset column_name feature_type [limit]
# MAGIC ```
# MAGIC 
# MAGIC This magic function is only available in python. It can be used from notebooks with other default languages by storing the intermediate result in a temporary view, and then adding a python cell that uses the `mosaic_kepler` with the temporary view created from another language.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Examples

# COMMAND ----------

# MAGIC %pip install databricks-mosaic --quiet

# COMMAND ----------

from pyspark.sql.functions import *
import mosaic as mos
mos.enable_mosaic(spark, dbutils)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Download example shapes

# COMMAND ----------

import requests

req = requests.get('https://data.cityofnewyork.us/api/geospatial/d3c5-ddgc?method=export&format=GeoJSON')
with open('/dbfs/tmp/nyc_taxi_zones.geojson', 'wb') as f:
  f.write(req.content)

# COMMAND ----------

neighbourhoods = (
  spark.read
    .option("multiline", "true")
    .format("json")
    .load("dbfs:/tmp/nyc_taxi_zones.geojson")
    
    # Extract geoJSON values for shapes
    .select("type", explode(col("features")).alias("feature"))
    .select("type", col("feature.properties").alias("properties"), to_json(col("feature.geometry")).alias("geom_json"))
    
    # Mosaic internal representation
    .withColumn("geom_internal", mos.st_geomfromgeojson("geom_json"))
  
    # WKT representation
    .withColumn("geom_wkt", mos.st_aswkt(col("geom_internal")))
  
    # WKB representation
    .withColumn("geom_wkb", mos.st_aswkb(col("geom_internal")))
  
   # Limit to only 1 shape
   .limit(1)
)

neighbourhoods.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plot geometries from Spark dataset

# COMMAND ----------

# MAGIC %md
# MAGIC #### Internal geometry type

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC neighbourhoods "geom_internal" "geometry"

# COMMAND ----------

# MAGIC %md
# MAGIC ![mosaic kepler map example geometry](../images/kepler-1.png)

# COMMAND ----------

# MAGIC %md
# MAGIC #### WKT geometry type

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC neighbourhoods "geom_wkt" "geometry"

# COMMAND ----------

# MAGIC %md
# MAGIC ![mosaic kepler map example geometry](../images/kepler-1.png)

# COMMAND ----------

# MAGIC %md
# MAGIC #### WKB geometry type

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC neighbourhoods "geom_wkb" "geometry"

# COMMAND ----------

# MAGIC %md
# MAGIC ![mosaic kepler map example geometry](../images/kepler-1.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plot geometries from table/view

# COMMAND ----------

neighbourhoods.createOrReplaceTempView("temp_view_neighbourhoods")

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC "temp_view_neighbourhoods" "geom_wkt" "geometry"

# COMMAND ----------

# MAGIC %md
# MAGIC ![mosaic kepler map example geometry](../images/kepler-1.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plot H3 indexes

# COMMAND ----------

neighbourhood_chips = (neighbourhoods
                       .limit(1)
                       .select(mos.grid_tessellateexplode("geom_internal", lit(9)))
                       .select("index.*")
                    )

neighbourhood_chips.show()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC neighbourhood_chips "index_id" "h3"

# COMMAND ----------

# MAGIC %md
# MAGIC ![mosaic kepler map example H3 indexes](../images/kepler-2.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plot H3 chips

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC neighbourhood_chips "wkb" "geometry"

# COMMAND ----------

# MAGIC %md
# MAGIC ![mosaic kepler map example H3 chips](../images/kepler-3.png)
