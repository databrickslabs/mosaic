# Databricks notebook source
# MAGIC %md # BNG: Transform + Join
# MAGIC
# MAGIC > Example of transforming WGS84 (EPSG:4326) into British National Grid (EPSG:27700), performing a spatial point-in-polygon join, and then generating a heat map of the results.
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
# MAGIC  __Last Update__ 28 NOV 2023 [Mosaic 0.3.12]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Mosaic
# MAGIC
# MAGIC > Mosaic framework is available via pip install and it comes with bindings for Python, SQL, Scala and R. The wheel file coming with pip installation is registering any necessary jars for other language support.

# COMMAND ----------

# MAGIC %pip install "databricks-mosaic<0.4,>=0.3" --quiet # <- Mosaic 0.3 series
# MAGIC # %pip install "databricks-mosaic<0.5,>=0.4" --quiet # <- Mosaic 0.4 series (as available)

# COMMAND ----------

# MAGIC %md ### Enable Mosaic in the notebook
# MAGIC
# MAGIC > To get started, you'll need to attach the wheel to your cluster and import instances as in the cell below. The defautl grid index system is set to H3. In order to use British National Grid you'll need to set the configuration parameter.

# COMMAND ----------

# -- configure AQE for more compute heavy operations
#  - choose option-1 or option-2 below, essential for REPARTITION!
# spark.conf.set("spark.databricks.optimizer.adaptive.enabled", False) # <- option-1: turn off completely for full control
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False) # <- option-2: just tweak partition management
spark.conf.set("spark.sql.shuffle.partitions", 1_024)                  # <-- default is 200

# -- import databricks + spark functions
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import *

# -- setup mosaic
import mosaic as mos

spark.conf.set("spark.databricks.labs.mosaic.index.system", "BNG")
mos.enable_mosaic(spark, dbutils)
# mos.enable_gdal(spark) # <- not needed for this example

# --other imports
import os
import pathlib
import requests
import zipfile
import warnings

warnings.simplefilter("ignore")

# COMMAND ----------

# MAGIC %md ### Setup Catalog + Schema
# MAGIC
# MAGIC > You will want to adjust for your environment.

# COMMAND ----------

catalog_name = "mjohns"
sql(f"USE CATALOG {catalog_name}")

db_name = "london_cycling"
sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
sql(f"USE SCHEMA {db_name}")

# COMMAND ----------

# MAGIC %md ## Setup Data
# MAGIC
# MAGIC > The download snippets are setup to only download 1x.

# COMMAND ----------

user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

data_dir = f"/tmp/mosaic/{user_name}"
print(f"Initial data stored in '{data_dir}'")

# COMMAND ----------

# MAGIC %md ### Initial London Postcodes [177]
# MAGIC
# MAGIC > Make sure we have London Postcode shapes available in our environment.

# COMMAND ----------

postcodes_dir = f"{data_dir}/postcodes"
postcodes_dir_fuse = f"/dbfs{postcodes_dir}"
dbutils.fs.mkdirs(postcodes_dir)

os.environ['POSTCODES_DIR_FUSE'] = postcodes_dir_fuse
print(f"POSTCODES_DIR_FUSE? '{postcodes_dir_fuse}'")

# COMMAND ----------

# MAGIC %sh 
# MAGIC wget -P $POSTCODES_DIR_FUSE -nc  https://raw.githubusercontent.com/databrickslabs/mosaic/main/notebooks/data/London_Postcode_Zones.geojson
# MAGIC ls -lh $POSTCODES_DIR_FUSE

# COMMAND ----------

# MAGIC %md _Load Postcode Polygons from GeoJSON_

# COMMAND ----------

postcodes = (
  spark.read
    .option("multiline", "true")
    .format("json")
    .load(postcodes_dir)
    .select("type", explode(col("features")).alias("feature"))
    .select("type", col("feature.properties").alias("properties"), to_json(col("feature.geometry")).alias("json_geometry"))
    .withColumn("geometry", mos.st_geomfromgeojson("json_geometry")) 
)
print(f"count? {postcodes.count():,}")
postcodes.limit(1).display() # <- limiting for ipynb only

# COMMAND ----------

# MAGIC %md ### Initial London Cycling Data [~40M]
# MAGIC
# MAGIC > We will setup a temporary location to store our UPRN data and then generate a table.

# COMMAND ----------

uprns_dir = f"{data_dir}/cycling"
uprns_dir_fuse = f"/dbfs{uprns_dir}"
dbutils.fs.mkdirs(uprns_dir)

os.environ['UPRNS_DIR_FUSE'] = uprns_dir_fuse
print(f"UPRNS_DIR_FUSE? '{uprns_dir_fuse}'")

# COMMAND ----------

uprns_url = 'https://api.os.uk/downloads/v1/products/OpenUPRN/downloads?area=GB&format=CSV&redirect'

# The DBFS file system is mounted under /dbfs/ directory on Databricks cluster nodes
uprns_dir_fuse_path = pathlib.Path(uprns_dir_fuse)
uprns_dir_fuse_path.mkdir(parents=True, exist_ok=True)

uprns_zip_fuse_path = uprns_dir_fuse_path / 'uprns.zip'
if not uprns_zip_fuse_path.exists():
  req = requests.get(uprns_url)
  with open(uprns_zip_fuse_path, 'wb') as f:
    f.write(req.content)
else:
  print(f"...skipping '{uprns_zip_fuse_path}', already exits.")

display(dbutils.fs.ls(uprns_dir))

# COMMAND ----------

uprns_data_fuse_path = uprns_dir_fuse_path / 'data'
uprns_data_fuse_path.mkdir(parents=True, exist_ok=True)
with zipfile.ZipFile(uprns_zip_fuse_path, 'r') as zip_ref:
    zip_ref.extractall(uprns_data_fuse_path)

# COMMAND ----------

# MAGIC %sh ls -lh $UPRNS_DIR_FUSE/data

# COMMAND ----------

csv_name = 'osopenuprn_202310.csv' # <- adjust to the name ^

# - alter csv columns
_df = (
  spark
    .read
      .option("header", "true")
      .option("inferSchema" , "true")
    .csv(f"{uprns_dir}/data/{csv_name}")
)
columns = [F.col(cn).alias(cn.replace(' ', '')) for cn in _df.columns]

# - write csv to table
spark.sql("drop table if exists uprns")
_df.select(*columns).write.format("delta").mode("overwrite").saveAsTable("uprns")

# COMMAND ----------

# MAGIC %md > We will load the Unique Property Reference Numbers (UPRNs) data to represent point data. 

# COMMAND ----------

uprns = spark.read.table("uprns")
print(f"count? {uprns.count():,}")  # <- faster after table gen
uprns.limit(10).display()           # <- limiting for ipynb only

# COMMAND ----------

# MAGIC %md ## Reproject Postcode Geometries to BNG SRID
# MAGIC > British National Grid expects coordinate of geometries to be provided in EPSG:27700. Our geometries are provided in EPSG:4326. So we will need to reproject the geometries. Mosaic has the necessary functionality to help us achieve this.

# COMMAND ----------

postcodes_bng = (
  postcodes.select(
    "type", "properties", "geometry"
  ).withColumn(
    "geometry", mos.st_setsrid("geometry", lit(4326))
  ).withColumn(
    "geometry", mos.st_transform("geometry", lit(27700))
  )
)
postcodes_bng.limit(1).display() # <- limiting for ipynb only

# COMMAND ----------

# MAGIC %md _Example: Compute some basic geometry attributes._
# MAGIC
# MAGIC > Mosaic provides a number of functions for extracting the properties of geometries. Below are some that are relevant to Polygon geometries:

# COMMAND ----------

display(
  postcodes_bng
    .withColumn("calculated_area", mos.st_area(col("geometry")))
    .withColumn("calculated_length", mos.st_length(col("geometry")))
    # Note: The unit of measure of the area and length depends on the CRS used.
    # For British National Grid locations it will be square meters and meters
    .select("geometry", "calculated_area", "calculated_length")
  .limit(1) # <- limiting for ipynb only
)

# COMMAND ----------

# MAGIC %md ## Reproject UPRN Points to BNG SRID
# MAGIC
# MAGIC > The UPRNs table contains Unique Property Reference Numbers and positions provided in EPSG:27700 and EPSG:4326. Since we are operating in EPSG:27700 and using BNG as our indexing system, we will use the location data provided via Northings and Eastings coordinates.

# COMMAND ----------

_uprns_bng = (
  uprns
    .withColumn("uprn_point", mos.st_point(col("X_COORDINATE"), col("Y_COORDINATE")))
    # we are using WKT here for simpler displaying, use WKB for faster query run time
    .withColumn("uprn_point", mos.st_aswkt("uprn_point")) 
    .where(mos.st_hasvalidcoordinates("uprn_point", lit('EPSG:27700'), lit('reprojected_bounds')))
    .where(mos.st_isvalid(col("uprn_point")))
    .drop("LATITUDE", "LONGITUDE")
)
_uprns_bng.limit(10).display() # <- limiting for ipynb only

# COMMAND ----------

# MAGIC %md
# MAGIC > Next step is optional. However, since we are constructing POINT geometries and ensuring they are valid it is prudent to write out the validated dataset. That way we are making sure validation is performed only once at ingestion time and not each time spark runs the queries (due to spark lazy evaluation). 

# COMMAND ----------

spark.sql("drop table if exists uprns_bng")
_uprns_bng.write.format("delta").mode("overwrite").saveAsTable("uprns_bng")

# COMMAND ----------

uprns_bng = spark.read.table("uprns_bng")
print(f"count? {uprns_bng.count():,}")

# COMMAND ----------

# MAGIC %md ## Spatial Joins
# MAGIC
# MAGIC > We can use Mosaic to perform spatial joins both with and without Mosaic indexing strategies. Indexing is very important when handling very different geometries both in size and in shape (ie. number of vertices). In the context of Mosaic we are using grid index systems rather than traditional tree based index system. The reason for this is the fact grid index systems like BNG and/or H3 are far better suited for distributed massive scale systems. Mosaic comes with grid_tessallate expressions that allow the caller to index an arbitrary shape within grid index system of choice. One thing to note here is that tessellation is a specialised way of converting a geometry to set of grid index system cells with their local geometries. </br>
# MAGIC
# MAGIC __Tessellation is applicable to any shape, Polygon, LineString, Points and their Multi* variants.__

# COMMAND ----------

# MAGIC %md ### [1] Getting the optimal resolution
# MAGIC
# MAGIC > We can use Mosaic functionality to identify how to best index/tessellate our data based on the data inside the specific dataframe. </br>
# MAGIC Selecting an apropriate tessellation resolution can have a considerable impact on the performance. </br>

# COMMAND ----------

from mosaic import MosaicFrame

postcodes_mosaic_frame = MosaicFrame(postcodes_bng, "geometry")
optimal_resolution = postcodes_mosaic_frame.get_optimal_resolution(sample_fraction=0.75)
optimal_resolution_str = postcodes_mosaic_frame.get_optimal_resolution_str(sample_fraction=0.75)

print(f"""
  Optimal resolution code is :{optimal_resolution}.
  Optimal resolution name is :{optimal_resolution_str}.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC > Not every resolution will yield performance improvements. By a rule of thumb it is always better to select more coarse resolution than to select a more fine grained resolution - if not sure select a lower resolution. Tessellation is a trade off between decomposition and explosion factor. The more fine grained the resolution is the more explosion of rows will impact the preprocessing time. However, it will make data more parallel. On the other hand, if the resolution is too coarse we are not addressing localisation related data skews. You can think of Mosaic's tessellation as a way to partition an overly complex row into multiple rows that have a balanced amount of computation each.

# COMMAND ----------

display(
  postcodes_mosaic_frame.get_resolution_metrics(sample_rows=150)
)

# COMMAND ----------

# MAGIC %md ### [2] Indexing/Tessellating using the optimal resolution
# MAGIC
# MAGIC > We will use mosaic sql functions to index our points data. Here we will use resolution -4 (500m), index resolution depends on the dataset in use. There is a second best choice which is 4 (100m). The user can pass either numerical resolution or the string label to the grid expressions. BNG provides 2 types of hierarchies. The standard hierarchy which operates with index resolutions in base 10 (i.e. (6, 1m), (5, 10m), (4, 100m), (3, 1km), (2, 10km), (1, 100km)) and cell ids follow the format of letter pair followed by coordinate bins at the selected resolution (e.g. TQ100100 for (4, 100m)). The quad hierachy (or quadrant hierarchy) which operates with index resolutions in base 5 (i.e. (-6, 5m), (-5, 50m), (-4, 500m), (-3, 5km), (-2, 50km), (-1, 500km)) and cell ids follow the format of letter pair followed by coordinate bins at the selected resolution and folowed by quadrant letters (e.g. TQ100100SW for (-4, 500m)). Quadrants correspond to compas directions SW (south west), NW (north west), NE (north east) and SE (south east).

# COMMAND ----------

# MAGIC %md _Full processing of all the Cycling data can take a little while, so we offer you a full or sample option, depending on your appetite._

# COMMAND ----------

uprns_opt = (
  uprns_bng
    .withColumn("uprn_bng_500m", mos.grid_pointascellid("uprn_point", F.lit(optimal_resolution)))
    .withColumn("uprn_bng_500m_str", mos.grid_pointascellid("uprn_point", F.lit(optimal_resolution_str)))
    .withColumn("uprn_bng_100m_str", mos.grid_pointascellid("uprn_point", F.lit("100m")))
)
# - uncomment to only use a 1% sample
#   using 400K of the 40M
uprns_opt = uprns_opt.sample(0.01)

print(f"count? {uprns_opt.count():,}")
uprns_opt.limit(10).display() # <- limiting for ipynb only

# COMMAND ----------

# MAGIC %md
# MAGIC > Mosaic has a builtin wrappers for KeplerGL map plots using mosaic_kepler IPython magics. Mosaic magics automatically handle bng grid idex system and CRS conversion for you. Given that Kepler Plots are rendered on the browser side we are automatically limiting the row count to 1000. The end user can override the number of ploted rows by specifying the desired number.

# COMMAND ----------

spark.catalog.clearCache() # <- cache is useful for dev (avoid recomputes)
count_per_index = uprns_opt.groupBy("uprn_bng_500m").count().cache()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC count_per_index "uprn_bng_500m" "bng" 50

# COMMAND ----------

# MAGIC %md
# MAGIC > We will use Mosaic to tessellate our postcode geometries using a built in tessellation generator (explode) function .

# COMMAND ----------

postcodes_with_index = (
  postcodes_bng
    # We break down the original geometry in multiple smaller mosaic chips
    # each fully contained in a grid cell
    .withColumn("chips", mos.grid_tessellateexplode(col("geometry"), F.lit(optimal_resolution)))
    # We don't need the original geometry any more, since we have broken it down into
    # Smaller mosaic chips.
    .drop("json_geometry", "geometry")
)

postcodes_with_index.limit(10).display() # <- limiting for ipynb only

# COMMAND ----------

to_display = postcodes_with_index.select("properties.Name", "chips.index_id", mos.st_aswkt("chips.wkb").alias("geometry"))

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC to_display "geometry" "geometry(27700)" 200

# COMMAND ----------

# MAGIC %md ### [3] Performing the spatial join
# MAGIC
# MAGIC > We can now do spatial join between our UPRNs and postcodes.

# COMMAND ----------

with_postcodes = (
  uprns_opt.join(
    postcodes_with_index,
    uprns_opt["uprn_bng_500m"] == postcodes_with_index["chips.index_id"],
    how = "right_outer" # to perserve even emtpy chips
  ).where(
    # If the borough is a core chip (the chip is fully contained within the geometry), then we do not need
    # to perform any intersection, because any point matching the same index will certainly be contained in
    # the borough. Otherwise we need to perform an st_contains operation on the chip geometry.
    col("chips.is_core") | mos.st_contains(col("chips.wkb"), col("uprn_point"))
  ).select(
    "properties.*", "uprn_point", "UPRN", "chips.index_id", mos.st_aswkt("chips.wkb").alias("index_geometry")
  )
)

with_postcodes.limit(1).display() # <- limiting for ipynb only

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualise the results in Kepler
# MAGIC
# MAGIC > Mosaic abstracts interaction with Kepler in python through mosaic_kepler magic. Mosaic_kepler magic takes care of conversion between EPSG:27700 and EPSG:4326 so that Kepler can properly render. It can handle columns with bng index ids (int and str formats are both supported) and geometries that are provided in EPSG:27700. Mosaic will convert all the geometries for proper rendering.

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC with_postcodes "index_geometry" "geometry(27700)" 5000

# COMMAND ----------

# MAGIC %md
# MAGIC > Using mosaic it takes only a few lines of code to produce BNG based heat map and visualise it in Kepler. By default the colors wont be affected by the counts and you'd need to change the options in Kepler UI. Navigate to the layer, expland it and for the fill color click on the 3 dots icon, then select count as the field for color scaling. 

# COMMAND ----------

properties_per_index = with_postcodes.groupBy("index_id").count()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC properties_per_index "index_id" "bng" 6000

# COMMAND ----------

# MAGIC %md
# MAGIC > We can do the same per chip.

# COMMAND ----------

properties_per_chip = with_postcodes.groupBy("index_geometry").count()

# COMMAND ----------

# MAGIC %md
# MAGIC > Note that if you dont use "right_outer" join some chips may be empty. This is due to no UPRNs being located in those exact chips.

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC properties_per_chip "index_geometry" "geometry(27700)" 20000
