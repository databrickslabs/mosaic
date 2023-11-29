# Databricks notebook source
# MAGIC %md ## Setup
# MAGIC
# MAGIC > Generates the table 'harbours_h3' in database 'ship2ship'.
# MAGIC
# MAGIC <p/>
# MAGIC
# MAGIC 1. Import Databricks columnar functions (including H3) for DBR / DBSQL Photon with `from pyspark.databricks.sql.functions import *`
# MAGIC 2. To use Databricks Labs [Mosaic](https://databrickslabs.github.io/mosaic/index.html) library for geospatial data engineering, analysis, and visualization functionality:
# MAGIC   * Install with `%pip install databricks-mosaic`
# MAGIC   * Import and use with the following:
# MAGIC   ```
# MAGIC   import mosaic as mos
# MAGIC   mos.enable_mosaic(spark, dbutils)
# MAGIC   ```
# MAGIC <p/>
# MAGIC
# MAGIC 3. To use [KeplerGl](https://kepler.gl/) OSS library for map layer rendering:
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
import os
import warnings

warnings.simplefilter("ignore")

# COMMAND ----------

# MAGIC %md __Configure Database__
# MAGIC
# MAGIC > Note: Adjust this to your own specified [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/admin-privileges.html#managing-unity-catalog-metastores) Schema.

# COMMAND ----------

catalog_name = "mjohns"
sql(f"use catalog {catalog_name}")

db_name = "ship2ship"
sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
sql(f"use schema {db_name}")

# COMMAND ----------

# MAGIC %md __AIS Data Download: `ETL_DIR` + `ETL_DIR_FUSE`__
# MAGIC
# MAGIC > Downloading initial data into a temp location. After the Delta Tables have been created, this location can be removed. You can alter this, of course, to match your preferred location. __Note:__ this is showing DBFS for continuity outside Unity Catalog + Shared Access clusters, but you can easily modify paths to use [Volumes](https://docs.databricks.com/en/sql/language-manual/sql-ref-volumes.html), see more details [here](https://databrickslabs.github.io/mosaic/usage/installation.html) as available.

# COMMAND ----------

ETL_DIR = '/tmp/ship2ship'
ETL_DIR_FUSE = f'/dbfs{ETL_DIR}'

os.environ['ETL_DIR'] = ETL_DIR
os.environ['ETL_DIR_FUSE'] = ETL_DIR_FUSE

dbutils.fs.mkdirs(ETL_DIR)
print(f"...ETL_DIR: '{ETL_DIR}', ETL_DIR_FUSE: '{ETL_DIR_FUSE}' (create)")

# COMMAND ----------

# MAGIC %sh
# MAGIC # see: https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2018/index.html
# MAGIC # - [1] we download data locally and unzip
# MAGIC mkdir /ship2ship/
# MAGIC cd /ship2ship/
# MAGIC wget -np -r -nH -L --cut-dirs=4 -nc https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2018/AIS_2018_01_31.zip > /dev/null 2>&1
# MAGIC unzip AIS_2018_01_31.zip
# MAGIC
# MAGIC # - [2] then copy to dbfs:// fuse mountpoint (/dbfs)
# MAGIC mv AIS_2018_01_31.csv $ETL_DIR_FUSE
# MAGIC ls -lh $ETL_DIR_FUSE

# COMMAND ----------

schema = """
  MMSI int, 
  BaseDateTime timestamp, 
  LAT double, 
  LON double, 
  SOG double, 
  COG double, 
  Heading double, 
  VesselName string, 
  IMO string, 
  CallSign string, 
  VesselType int, 
  Status int, 
  Length int, 
  Width int, 
  Draft double, 
  Cargo int, 
  TranscieverClass string
"""

AIS_df = (
    spark.read.csv(ETL_DIR, header=True, schema=schema)
    .filter("VesselType = 70")    # <- only select cargos
    .filter("Status IS NOT NULL")
)
display(AIS_df.limit(5))          # <- limiting for ipynb only

# COMMAND ----------

(AIS_df.write.format("delta").mode("overwrite").saveAsTable("AIS"))

# COMMAND ----------

# MAGIC %sql select format_number(count(1), 0) as count from AIS

# COMMAND ----------

# MAGIC %md ## Harbours
# MAGIC
# MAGIC This data can be obtained from [here](https://data-usdot.opendata.arcgis.com/datasets/usdot::ports-major/about), and loaded with the code below.
# MAGIC
# MAGIC To avoid detecting overlap close to, or within harbours, in Notebook `03.b Advanced Overlap Detection` we filter out events taking place close to a harbour.
# MAGIC Various approaches are possible, including filtering out events too close to shore, and can be implemented in a similar fashion.
# MAGIC
# MAGIC In this instance we set a buffer of `10 km` around harbours to arbitrarily define an area wherein we do not expect ship-to-ship transfers to take place.
# MAGIC Since our projection is not in metres, we convert from decimal degrees. With `(0.00001 - 0.000001)` as being equal to one metre at the equator
# MAGIC Ref: http://wiki.gis.com/wiki/index.php/Decimal_degrees

# COMMAND ----------

# MAGIC %sh
# MAGIC # we download data to dbfs:// mountpoint (/dbfs)
# MAGIC cd $ETL_DIR_FUSE && \
# MAGIC   wget -np -r -nH -L -q --cut-dirs=7 -O harbours.geojson -nc "https://geo.dot.gov/mapping/rest/services/NTAD/Strategic_Ports/MapServer/0/query?outFields=*&where=1%3D1&f=geojson"
# MAGIC
# MAGIC ls -lh $ETL_DIR_FUSE

# COMMAND ----------

one_metre = 0.00001 - 0.000001
buffer = 10 * 1000 * one_metre

major_ports = (
    spark.read.format("json")
    .option("multiline", "true")
    .load(f"{ETL_DIR}/harbours.geojson")
    .select("type", F.explode(col("features")).alias("feature"))
    .select(
        "type",
        col("feature.properties").alias("properties"),
        F.to_json(col("feature.geometry")).alias("json_geometry"),
    )
    .withColumn("geom", mos.st_aswkt(mos.st_geomfromgeojson("json_geometry")))
    .select(col("properties.PORT_NAME").alias("name"), "geom")
    .withColumn("geom", mos.st_buffer("geom", F.lit(buffer)))
)
major_ports.limit(1).display() # <- limiting for ipynb only

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC major_ports "geom" "geometry"

# COMMAND ----------

(
    major_ports.select("name", mos.grid_tessellateexplode("geom", F.lit(9)).alias("mos"))
    .select("name", col("mos.index_id").alias("h3"))
    .write.mode("overwrite")
    .format("delta")
    .saveAsTable("harbours_h3")
)

# COMMAND ----------

harbours_h3 = spark.read.table("harbours_h3")
display(harbours_h3.limit(5)) # <- limiting for ipynb only

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC "harbours_h3" "h3" "h3" 5_000
