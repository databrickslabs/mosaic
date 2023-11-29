# Databricks notebook source
# MAGIC %md # Mosaic & Sedona
# MAGIC
# MAGIC > You can combine the usage of Mosaic with other geospatial libraries. In this example we combine the use of [Sedona](https://sedona.apache.org) and Mosaic.
# MAGIC
# MAGIC ## Setup
# MAGIC
# MAGIC This notebook will run if you have both Mosaic and Sedona installed on your cluster.
# MAGIC
# MAGIC ### Install sedona
# MAGIC
# MAGIC To install Sedona, follow the [official Sedona instructions](https://sedona.apache.org/1.5.0/setup/databricks/).
# MAGIC
# MAGIC E.g. Add the following maven coorinates to a non-photon cluster [[1](https://docs.databricks.com/en/libraries/package-repositories.html)]. This is showing DBR 12.2 LTS.
# MAGIC
# MAGIC ```
# MAGIC org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.5.0
# MAGIC org.datasyslab:geotools-wrapper:1.5.0-28.2
# MAGIC ```
# MAGIC
# MAGIC __Note:__ See instructions for `SedonaContext.create(spark)` [[1](https://sedona.apache.org/1.5.0/tutorial/sql/?h=sedonacontext#initiate-sedonacontext)]. Also, notice we are downgrading pandas from default DBR version for Sedona.
# MAGIC
# MAGIC ---
# MAGIC  __Last Update__ 28 NOV 2023 [Mosaic 0.3.12]

# COMMAND ----------

# MAGIC %pip install "pandas<=1.3.5" "shapely<= 1.8.4" "geopandas<=0.10.2" keplergl==0.3.2 pydeck==0.8.0 --quiet # <- Sedona 1.5 dep versions
# MAGIC %pip install "apache-sedona<1.6,>=1.5" --quiet                                                           # <- Sedona 1.5 series
# MAGIC %pip install "databricks-mosaic<0.4,>=0.3" --quiet                                                       # <- Mosaic 0.3 series [install last]

# COMMAND ----------

# MAGIC %md _Verify our Sedona dependency versions_

# COMMAND ----------

import pandas as pd
import shapely
import geopandas as gpd
import keplergl

print(f"pandas version? {pd.__version__}")
print(f"geopandas version? {gpd.__version__}")
print(f"shapely version? {shapely.__version__}")
print(f"kepler version? {keplergl.__version__}")

# COMMAND ----------

# MAGIC %md _Main imports_

# COMMAND ----------

import pyspark.sql.functions as F

# -- setup mosaic
import mosaic as mos

mos.enable_mosaic(spark, dbutils)

# -- setup sedona
from sedona.spark import *

sedona = SedonaContext.create(spark)

# --other imports
import warnings

warnings.simplefilter("ignore")

# COMMAND ----------

# MAGIC %md _Setup simple DataFrame_
# MAGIC
# MAGIC > Showing blending Mosaic calls (namespaced as `mos.`) with Sedona (sql) calls.

# COMMAND ----------

df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
(df
   # Mosaic
   .withColumn("mosaic_area", mos.st_area('wkt'))
   # Sedona
   .withColumn("sedona_area", F.expr("ST_Area(ST_GeomFromWKT(wkt))"))
   # Sedona function not available in Mosaic
   .withColumn("sedona_flipped", F.expr("ST_FlipCoordinates(ST_GeomFromWKT(wkt))"))
).show()

# COMMAND ----------

# MAGIC %md ## Mosaic + Kepler
# MAGIC
# MAGIC > Mosaic has the ability to render tables / views + dataframes with `%%mosaic_kepler` magic [[1](https://databrickslabs.github.io/mosaic/usage/kepler.html)].

# COMMAND ----------

%%mosaic_kepler
df "wkt" "geometry"

# COMMAND ----------

# MAGIC %md ## Sedona
# MAGIC
# MAGIC > Converting to a Sedona DataFrame. __Note: there are a few ways to do this.__

# COMMAND ----------

# MAGIC %md _[1] Spark DataFrame `df` to Pandas DataFrame `pdf`._

# COMMAND ----------

pdf = df.toPandas()
pdf

# COMMAND ----------

# MAGIC %md _[2] Pandas DataFrame `pdf` to GeoPandas DataFrame `gdf`._

# COMMAND ----------

gdf = gpd.GeoDataFrame(
    pdf, geometry=gpd.geoseries.from_wkt(pdf['wkt'], crs="EPSG:4326")
)
gdf.drop('wkt', axis=1, inplace=True)
gdf

# COMMAND ----------

# MAGIC %md _[3] GeoPandas DataFrame `gdf` to Sedona DataFrame `sdf`._

# COMMAND ----------

sdf = sedona.createDataFrame(gdf)
sdf.show()

# COMMAND ----------

# MAGIC %md ### SedonaKepler
# MAGIC
# MAGIC > Sedona also has some ability to render Kepler [[1](https://sedona.apache.org/1.5.0/api/sql/Visualization_SedonaKepler/)].

# COMMAND ----------

SedonaKepler.create_map(df=sdf, name="MySedona")

# COMMAND ----------

# MAGIC %md ### SedonaPyDeck
# MAGIC
# MAGIC > Sedona also has a pydeck renderer [[1](https://sedona.apache.org/1.5.0/api/sql/Visualization_SedonaPyDeck/)].

# COMMAND ----------

SedonaPyDeck.create_geometry_map(sdf)
