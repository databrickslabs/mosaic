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
# MAGIC Luckily, Mosaic has the necessary functionality to help us achieve this.

# COMMAND ----------

postcodes = (
  postcodes.select(
    "type", "properties", "geometry"
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
# MAGIC That way we are making sure validation is performed only once ate ingestion and not each time spark runs the queries (due to spark lazy evaluation). 

# COMMAND ----------

uprns_table.write.format("delta").saveAsTable("uprns_bng_table")

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting the optimal resolution

# COMMAND ----------

# MAGIC %md
# MAGIC We can use Mosaic functionality to identify how to best index our data based on the data inside the specific dataframe. </br>
# MAGIC Selecting an apropriate indexing resolution can have a considerable impact on the performance. </br>

# COMMAND ----------

from mosaic import MosaicFrame
from mosaic import MosaicContext

mosaic_context = MosaicContext(spark)
bng = mosaic_context._context.indexSystem()

postcodes_mosaic_frame = MosaicFrame(postcodes, "geometry")
optimal_resolution = postcodes_mosaic_frame.get_optimal_resolution(sample_fraction=0.75)
optimal_resolution_str = bng.getResolutionStr(optimal_resolution)

print(f"""
  Optimal resolution code is :{optimal_resolution}.
  Optimal resolution name is :{optimal_resolution_str}.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Not every resolution will yield performance improvements. </br>
# MAGIC By a rule of thumb it is always better to under-index than over-index - if not sure select a lower resolution. </br>
# MAGIC Higher resolutions are needed when we have very imballanced geometries with respect to their size or with respect to the number of vertices. </br>
# MAGIC In such case indexing with more indices will considerably increase the parallel nature of the operations. </br>
# MAGIC You can think of Mosaic as a way to partition an overly complex row into multiple rows that have a balanced amount of computation each.

# COMMAND ----------

display(
  postcodes_mosaic_frame.get_resolution_metrics(sample_rows=150)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Indexing using the optimal resolution

# COMMAND ----------

# MAGIC %md
# MAGIC We will use mosaic sql functions to index our points data. </br>
# MAGIC Here we will use resolution -4 (500m), index resolution depends on the dataset in use. <br>
# MAGIC There is a second best choice which is 4 (100m). <br> 
# MAGIC BNG provides 2 types of hieracies. <br> 
# MAGIC The standard hierarchy which operates with index resolutions in base 10 (i.e. (6, 1m), (5, 10m), (4, 100m), (3, 1km), (2, 10km), (1, 100km)) and index ids follow the format of letter pair followed by coordinate bins at the selected resolution (e.g. TQ100100 for (4, 100m)). <br>
# MAGIC The quad hierachy (or quadrant hierarchy) which operates with index resolutions in base 5 (i.e. (-6, 5m), (-5, 50m), (-4, 500m), (-3, 5km), (-2, 50km), (-1, 500km)) and index ids follow the format of letter pair followed by coordinate bins at the selected resolution and folowed by quadrant letters (e.g. TQ100100SW for (-4, 500m)). Quadrants correspond to compas directions SW (south west), NW (north west), NE (north east) and SE (south east). <br>

# COMMAND ----------

uprns_table = spark.read.table("uprns_bng_table")
uprns_table = (
  uprns_table
    .withColumn("uprn_bng_500m", mos.grid_pointascellid("uprn_point", lit(optimal_resolution)))
    .withColumn("uprn_bng_100m", mos.grid_pointascellid("uprn_point", lit(-optimal_resolution)))
)

# COMMAND ----------

uprns_table.display()

# COMMAND ----------

import re

import h3
import pandas as pd
from IPython.core.magic import Magics, cell_magic, magics_class
from keplergl import KeplerGl
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, conv, lower, lit

from mosaic.api.accessors import st_astext, st_aswkt
from mosaic.api.constructors import st_geomfromwkt, st_geomfromwkb
from mosaic.api.functions import st_centroid2D, grid_pointascellid, grid_boundaryaswkb, st_setsrid, st_transform
from mosaic.config import config
from mosaic.utils.kepler_config import mosaic_kepler_config


external_ref_map = None

@magics_class
class MosaicKepler2(Magics):
    @staticmethod
    def displayKepler(map_instance, height, width):
        decoded = (
            map_instance._repr_html_()
            .decode("utf-8")
            .replace(".height||400", f".height||{height}")
            .replace(".width||400", f".width||{width}")
        )
        ga_script_redacted = re.sub(
            r"\<script\>\(function\(i,s,o,g,r,a,m\).*?GoogleAnalyticsObject.*?(\<\/script\>)",
            "",
            decoded,
            flags=re.DOTALL,
        )
        async_script_redacted = re.sub(
            r"s\.a\.createElement\(\"script\",\{async.*?\}\),",
            "",
            ga_script_redacted,
            flags=re.DOTALL,
        )
        config.notebook_utils.displayHTML(async_script_redacted)

    @staticmethod
    def get_spark_df(table_name):
        try:
            table = config.ipython_hook.ev(table_name)
            if isinstance(table, pd.DataFrame):  # pandas dataframe
                data = config.mosaic_spark.createDataFrame(table)
            elif isinstance(table, DataFrame):  # spark dataframe
                data = table
            else:  # table name
                data = config.mosaic_spark.read.table(table)
        except NameError:
            try:
                data = config.mosaic_spark.read.table(table_name)
            except:
                raise Exception(f"Table name reference invalid.")
        return data

    @staticmethod
    def set_centroid(pandas_data, feature_type, feature_name):
        tmp_sdf = config.mosaic_spark.createDataFrame(pandas_data.iloc[:1])
        
        if feature_type == "h3":
            tmp_sdf = tmp_sdf.withColumn(feature_name, grid_boundaryaswkb(feature_name))
            
        centroid = (
            tmp_sdf.select(st_centroid2D(feature_name))
            .limit(1)
            .collect()[0][0]
        )
            
        # set to centroid of a geom
        mosaic_kepler_config["config"]["mapState"]["latitude"] = centroid[1]
        mosaic_kepler_config["config"]["mapState"]["longitude"] = centroid[0]

    @cell_magic
    def mosaic_kepler2(self, *args):
        """Replace current line with new output"""
        inputs = [
            i
            for i in " ".join(list(args)).replace("\n", " ").replace('"', "").split(" ")
            if len(i) > 0
        ]

        if len(inputs) != 3 and len(inputs) != 4:
            raise Exception(
                "Mosaic Kepler magic requires table name, feature column and feature type all to be provided. "
                + "Limit is optional (default 1000)."
            )

        table_name = inputs[0]
        feature_name = inputs[1]
        feature_type = inputs[2]
        limit_ctn = 1000
        if len(inputs) == 4:
            limit_ctn = int(inputs[3])
        data = self.get_spark_df(table_name)
        feature_col_dt = [dt for dt in data.dtypes if dt[0] == feature_name][0]

        if feature_type == "h3":
            if feature_col_dt[1] == "bigint":
                data = data.withColumn(
                    feature_name, lower(conv(col(feature_name), 10, 16))
                )
        if feature_type == "bng":
            data = (data
                .withColumn(feature_name, grid_boundaryaswkb(feature_name))
                .withColumn(feature_name, st_geomfromwkb(feature_name))
                .withColumn(
                    feature_name, 
                    st_transform(st_setsrid(feature_name, lit(27700)), lit(4326)) 
                )
                .withColumn(feature_name, st_aswkt(feature_name)))
        elif feature_type == "geometry":
            data = data.withColumn(feature_name, st_astext(col(feature_name)))
        elif feature_type == "bng_geometry":
            data = (data
                .withColumn(feature_name, st_geomfromwkt(st_aswkt(feature_name)))
                .withColumn(
                    feature_name, 
                    st_transform(st_setsrid(feature_name, lit(27700)), lit(4326)) 
                )
                .withColumn(feature_name, st_aswkt(feature_name)))
        else:
            raise Exception(f"Unsupported geometry type: {feature_type}.")

        allowed_schema = [
            field.name
            for field in data.schema.fields
            if field.dataType.typeName() in ["string", "long", "integer", "double"]
        ]
        data = data.select(*allowed_schema)
        pandas_data = data.limit(limit_ctn).toPandas()

        self.set_centroid(pandas_data, feature_type, feature_name)

        m1 = KeplerGl(config=mosaic_kepler_config)
        m1.add_data(data=pandas_data, name=table_name)
        
        external_ref_map = m1

        self.displayKepler(m1, 800, 1200)


# COMMAND ----------

mosaic_kepler_config

# COMMAND ----------

get_ipython().register_magics(MosaicKepler2)

# COMMAND ----------

count_per_index = uprns_table.groupBy("uprn_bng_500m").count().cache()

# COMMAND ----------

count_per_index.display()

# COMMAND ----------

# MAGIC %%mosaic_kepler2
# MAGIC count_per_index "uprn_bng_500m" "bng" 50

# COMMAND ----------

# MAGIC %md
# MAGIC We will also index our postcodes using a built in generator function.

# COMMAND ----------

postcodes_with_index = (postcodes

                           # We break down the original geometry in multiple smoller mosaic chips, each with its
                           # own index
                           .withColumn("mosaic_index", mos.mosaic_explode(col("geometry"), lit(optimal_resolution)))

                           # We don't need the original geometry any more, since we have broken it down into
                           # Smaller mosaic chips.
                           .drop("json_geometry", "geometry")
                          )

# COMMAND ----------

postcodes_with_index.printSchema()

# COMMAND ----------

to_display = postcodes_with_index.select("properties.*", "mosaic_index.*")

# COMMAND ----------

# MAGIC %%mosaic_kepler2
# MAGIC to_display "index_id" "bng" 500

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
    uprns_table["uprn_bng_500m"] == postcodes_with_index["mosaic_index.index_id"]
  ).where(
    # If the borough is a core chip (the chip is fully contained within the geometry), then we do not need
    # to perform any intersection, because any point matching the same index will certainly be contained in
    # the borough. Otherwise we need to perform an st_contains operation on the chip geometry.
    col("mosaic_index.is_core") | mos.st_contains(col("mosaic_index.wkb"), col("uprn_point"))
  ).select(
    "properties.*", "uprn_point", "UPRN", "mosaic_index.index_id"
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
# MAGIC It can handle columns with bng index ids (int and str formats are both supported) and geometries that are provided in EPSG:27700. Mosaic will convert all the geometries for proper rendering. For CRSIDs that are neither EPSG:27700 nor EPSG:4326 the end user needs to convert the geometries using mos.st_transform(geom, destination_crsid).

# COMMAND ----------

# MAGIC %%mosaic_kepler2
# MAGIC with_postcodes "index_id" "bng" 5000

# COMMAND ----------

properties_per_index = with_postcodes.groupBy("index_id").count()

# COMMAND ----------

# MAGIC %%mosaic_kepler2
# MAGIC properties_per_index "index_id" "bng" 6000

# COMMAND ----------

postcodes.display()

# COMMAND ----------

properties_per_postcode = with_postcodes.groupBy("Name").count().join(
  postcodes.select(col("properties.Name"), mos.st_astext("geometry").alias("geometry")), on=["Name"]
)

# COMMAND ----------

# MAGIC %%mosaic_kepler2
# MAGIC properties_per_postcode "geometry" "bng_geometry"

# COMMAND ----------


