# Databricks notebook source
# MAGIC %md
# MAGIC ## Install Mosaic
# MAGIC Mosaic framework is available via pip install and it comes with bindings for Python, SQL, Scala and R. <br>
# MAGIC The wheel file coming with pip installation is registering any necessary jars for other language support.

# COMMAND ----------

# MAGIC %pip install databricks-mosaic

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup London Postcode zones
# MAGIC In order to setup the data please run the notebook available at "../../data/DownloadLondonPostcodeZones". </br>
# MAGIC DownloadLondonPostcodeZones notebook will make sure we have London Postcode shapes available in our environment.

# COMMAND ----------

user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

raw_path = f"dbfs:/tmp/mosaic/{user_name}"
raw_postcode_zones_path = f"{raw_path}/postcodes"

print(f"The raw data is stored in {raw_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enable Mosaic in the notebook
# MAGIC To get started, you'll need to attach the wheel to your cluster and import instances as in the cell below. <br>
# MAGIC The defautl grid index system is set to H3. In oreder to use British National Grid you'd need to set the configuration parameter. <br>

# COMMAND ----------

from pyspark.sql.functions import *
import mosaic as mos
spark.conf.set("spark.databricks.labs.mosaic.index.system", "BNG")
mos.enable_mosaic(spark, dbutils)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Read polygons from GeoJson

# COMMAND ----------

postcodes = (
  spark.read
    .option("multiline", "true")
    .format("json")
    .load(raw_postcode_zones_path)
    .select("type", explode(col("features")).alias("feature"))
    .select("type", col("feature.properties").alias("properties"), to_json(col("feature.geometry")).alias("json_geometry"))
    .withColumn("geometry", mos.st_geomfromgeojson("json_geometry")) 
)

display(
  postcodes
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Reproject the geometries to correct SRID
# MAGIC British National Grid expects coordinate of geometries to be provided in EPSG:27700. <br>
# MAGIC Our geometries are provided in EPSG:4326. So we will need to reproject the geometries. <br>
# MAGIC Mosaic has the necessary functionality to help us achieve this.

# COMMAND ----------

postcodes = (
  postcodes.select(
    "type", "properties", "geometry"
  ).withColumn(
    "geometry", mos.st_setsrid("geometry", lit(4326))
  ).withColumn(
    "geometry", mos.st_transform("geometry", lit(27700))
  )
)

postcodes.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Compute some basic geometry attributes

# COMMAND ----------

# MAGIC %md
# MAGIC Mosaic provides a number of functions for extracting the properties of geometries. Here are some that are relevant to Polygon geometries:

# COMMAND ----------

display(
  postcodes
    .withColumn("calculated_area", mos.st_area(col("geometry")))
    .withColumn("calculated_length", mos.st_length(col("geometry")))
    # Note: The unit of measure of the area and length depends on the CRS used.
    # For British National Grid locations it will be square meters and meters
    .select("geometry", "calculated_area", "calculated_length")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read points data

# COMMAND ----------

# MAGIC %md
# MAGIC We will load the Unique Property Reference Numbers (UPRNs) data to represent point data. </br>
# MAGIC In order to setup the data please run the notebook available at "../../data/DownloadUPRNsData". </br>
# MAGIC DownloadUPRNsData notebook will make sure we have UPRN table with point data available in our environment.
# MAGIC We already loaded some shapes representing polygons that correspond to London postcodes. </br>

# COMMAND ----------

uprns_table = spark.table("uprns_table")
display(uprns_table)

# COMMAND ----------

# MAGIC %md
# MAGIC The UPRNs table contains Unique Property Reference Numbers and positions provided in EPSG:27700 and EPSG:4326. <br>
# MAGIC Since we are operating in EPSG:27700 and using BNG as our indexing system, we will use the location data provided via Northings and Eastings coordinates.

# COMMAND ----------

uprns_table = (
  uprns_table
    .withColumn("uprn_point", mos.st_point(col("X_COORDINATE"), col("Y_COORDINATE")))
    # we are using WKT here for simpler displaying, use WKB for faster query run time
    .withColumn("uprn_point", mos.st_aswkt("uprn_point")) 
    .where(mos.st_hasvalidcoordinates("uprn_point", lit('EPSG:27700'), lit('reprojected_bounds')))
    .where(mos.st_isvalid(col("uprn_point")))
    .drop("LATITUDE", "LONGITUDE")
)
display(uprns_table)

# COMMAND ----------

# MAGIC %md
# MAGIC Next step is optional. Howerver, since we are constructing POINT geometries and ensuring they are valid it is prudent to write out the validated dataset. <br>
# MAGIC That way we are making sure validation is performed only once at ingestion time and not each time spark runs the queries (due to spark lazy evaluation). 

# COMMAND ----------

uprns_table.write.format("delta").mode("overwrite").saveAsTable("uprns_bng_table")

# COMMAND ----------

uprns_table = spark.read.table("uprns_bng_table")

# COMMAND ----------

uprns_table.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spatial Joins

# COMMAND ----------

# MAGIC %md
# MAGIC We can use Mosaic to perform spatial joins both with and without Mosaic indexing strategies. </br>
# MAGIC Indexing is very important when handling very different geometries both in size and in shape (ie. number of vertices). </br>
# MAGIC In the context of Mosaic we are using grid index systems rather than traditional tree based index system. </br>
# MAGIC The reason for this is the fact grid index systems like BNG and/or H3 are far better suited for distributed massive scale systems. </br>
# MAGIC Mosaic comes with grid_tessallate expressions that allow the caller to index an arbitrary shape within grid index system of choice. </br>
# MAGIC One thing to note here is that tessellation is a specialised way of converting a geometry to set of grid index system cells with their local geometries. </br>
# MAGIC Tesselation is applicable to any shape, Polygon, LineString, Points and their Multi* variants. </br>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting the optimal resolution

# COMMAND ----------

# MAGIC %md
# MAGIC We can use Mosaic functionality to identify how to best index/tessellate our data based on the data inside the specific dataframe. </br>
# MAGIC Selecting an apropriate tesselation resolution can have a considerable impact on the performance. </br>

# COMMAND ----------

from mosaic import MosaicFrame

postcodes_mosaic_frame = MosaicFrame(postcodes, "geometry")
optimal_resolution = postcodes_mosaic_frame.get_optimal_resolution(sample_fraction=0.75)
optimal_resolution_str = postcodes_mosaic_frame.get_optimal_resolution_str(sample_fraction=0.75)

print(f"""
  Optimal resolution code is :{optimal_resolution}.
  Optimal resolution name is :{optimal_resolution_str}.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Not every resolution will yield performance improvements. </br>
# MAGIC By a rule of thumb it is always better to select more coarse resolution than to select a more fine grained resolution - if not sure select a lower resolution. </br>
# MAGIC Tessellation is a trade off between decomposition and explosion factor. </br>
# MAGIC The more fine grained the resolution is the more explosion of rows will impact the preprocessing time. However, it will make data more parallel. </br>
# MAGIC On the other hand, if the resolution is too coarse we are not addressing localisation related data skews. </br>
# MAGIC You can think of Mosaic's tesselation as a way to partition an overly complex row into multiple rows that have a balanced amount of computation each.

# COMMAND ----------

display(
  postcodes_mosaic_frame.get_resolution_metrics(sample_rows=150)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Indexing/Tessellating using the optimal resolution

# COMMAND ----------

# MAGIC %md
# MAGIC We will use mosaic sql functions to index our points data. </br>
# MAGIC Here we will use resolution -4 (500m), index resolution depends on the dataset in use. <br>
# MAGIC There is a second best choice which is 4 (100m). <br> 
# MAGIC The user can pass either numerical resolution or the string label to the grid expressions. <br>
# MAGIC BNG provides 2 types of hierarchies. <br> 
# MAGIC The standard hierarchy which operates with index resolutions in base 10 (i.e. (6, 1m), (5, 10m), (4, 100m), (3, 1km), (2, 10km), (1, 100km)) and cell ids follow the format of letter pair followed by coordinate bins at the selected resolution (e.g. TQ100100 for (4, 100m)). <br>
# MAGIC The quad hierachy (or quadrant hierarchy) which operates with index resolutions in base 5 (i.e. (-6, 5m), (-5, 50m), (-4, 500m), (-3, 5km), (-2, 50km), (-1, 500km)) and cell ids follow the format of letter pair followed by coordinate bins at the selected resolution and folowed by quadrant letters (e.g. TQ100100SW for (-4, 500m)). Quadrants correspond to compas directions SW (south west), NW (north west), NE (north east) and SE (south east). <br>

# COMMAND ----------

uprns_table = spark.read.table("uprns_bng_table")
uprns_table = (
  uprns_table
    .withColumn("uprn_bng_500m", mos.grid_pointascellid("uprn_point", lit(optimal_resolution)))
    .withColumn("uprn_bng_500m_str", mos.grid_pointascellid("uprn_point", lit(optimal_resolution_str)))
    .withColumn("uprn_bng_100m_str", mos.grid_pointascellid("uprn_point", lit("100m")))
)

# COMMAND ----------

uprns_table.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Mosaic has a builtin wrappers for KeplerGL map plots using mosaic_kepler IPython magics. <br>
# MAGIC Mosaic magics automatically handle bng grid idex system and CRS conversion for you. <br>
# MAGIC Given that Kepler Plots are rendered on the browser side we are automatically limiting the row count to 1000. <br>
# MAGIC The end user can override the number of ploted rows by specifying the desired number.

# COMMAND ----------

count_per_index = uprns_table.groupBy("uprn_bng_500m").count().cache()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC count_per_index "uprn_bng_500m" "bng" 50

# COMMAND ----------

# MAGIC %md
# MAGIC We will use Mosaic to tessellate our postcode geometries using a built in tesselation generator (explode) function .

# COMMAND ----------

postcodes_with_index = (postcodes

                           # We break down the original geometry in multiple smaller mosaic chips
                           # each fully contained in a grid cell
                           .withColumn("chips", mos.grid_tessellateexplode(col("geometry"), lit(optimal_resolution)))

                           # We don't need the original geometry any more, since we have broken it down into
                           # Smaller mosaic chips.
                           .drop("json_geometry", "geometry")
                          )

# COMMAND ----------

postcodes_with_index.display()

# COMMAND ----------

to_display = postcodes_with_index.select("properties.Name", "chips.index_id", mos.st_aswkt("chips.wkb").alias("geometry"))

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC to_display "geometry" "geometry(27700)" 200

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performing the spatial join

# COMMAND ----------

# MAGIC %md
# MAGIC We can now do spatial join between our UPRNs and postcodes.

# COMMAND ----------

with_postcodes = (
  uprns_table.join(
    postcodes_with_index,
    uprns_table["uprn_bng_500m"] == postcodes_with_index["chips.index_id"],
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

display(with_postcodes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualise the results in Kepler

# COMMAND ----------

# MAGIC %md
# MAGIC Mosaic abstracts interaction with Kepler in python through mosaic_kepler magic. <br>
# MAGIC Mosaic_kepler magic takes care of conversion between EPSG:27700 and EPSG:4326 so that Kepler can properly render. <br>
# MAGIC It can handle columns with bng index ids (int and str formats are both supported) and geometries that are provided in EPSG:27700. <br>
# MAGIC Mosaic will convert all the geometries for proper rendering.

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC with_postcodes "index_geometry" "geometry(27700)" 5000

# COMMAND ----------

# MAGIC %md
# MAGIC Using mosaic it takes only a few lines of code to produce BNG based heat map and visualise it in Kepler. <br>
# MAGIC By default the colors wont be affected by the counts and you'd need to change the options in Kepler UI. <br>
# MAGIC Navigate to the layer, expland it and for the fill color click on the 3 dots icon, then select count as the field for color scaling. 

# COMMAND ----------

properties_per_index = with_postcodes.groupBy("index_id").count()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC properties_per_index "index_id" "bng" 6000

# COMMAND ----------

# MAGIC %md
# MAGIC We can do the same 

# COMMAND ----------

properties_per_chip = with_postcodes.groupBy("index_geometry").count()

# COMMAND ----------

# MAGIC %md
# MAGIC Note that if you dont use "right_outer" join some chips may be empty. <br>
# MAGIC This is due to no UPRNs being located in those exact chips. <br>

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC properties_per_chip "index_geometry" "geometry(27700)" 20000

# COMMAND ----------


