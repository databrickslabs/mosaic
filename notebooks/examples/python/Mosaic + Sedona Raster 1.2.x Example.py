# Databricks notebook source
# MAGIC %md # Mosaic + Apache Sedona Raster 1.2.x Example
# MAGIC 
# MAGIC * Assumes DBR 10.4 which is Spark 3.2.1

# COMMAND ----------

# MAGIC %md _Example Cluster Config from demo shard (doesn't need to be single node)_
# MAGIC ```
# MAGIC {
# MAGIC     "num_workers": 0,
# MAGIC     "cluster_name": "mjohns-mosaic-sedona-sn",
# MAGIC     "spark_version": "10.4.x-scala2.12",
# MAGIC     "spark_conf": {
# MAGIC         "spark.databricks.cluster.profile": "singleNode",
# MAGIC         "spark.sql.extensions": "org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions",
# MAGIC         "spark.master": "local[*, 4]",
# MAGIC         "spark.kryo.registrator": "org.apache.sedona.core.serde.SedonaKryoRegistrator",
# MAGIC         "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
# MAGIC     },
# MAGIC     "aws_attributes": {
# MAGIC         "first_on_demand": 1,
# MAGIC         "availability": "SPOT_WITH_FALLBACK",
# MAGIC         "zone_id": "us-west-2c",
# MAGIC         "spot_bid_price_percent": 100,
# MAGIC         "ebs_volume_count": 0
# MAGIC     },
# MAGIC     "node_type_id": "i3.xlarge",
# MAGIC     "driver_node_type_id": "i3.xlarge",
# MAGIC     "ssh_public_keys": [],
# MAGIC     "custom_tags": {
# MAGIC         "ResourceClass": "SingleNode"
# MAGIC     },
# MAGIC     "spark_env_vars": {
# MAGIC         "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
# MAGIC     },
# MAGIC     "autotermination_minutes": 60,
# MAGIC     "enable_elastic_disk": true,
# MAGIC     "cluster_source": "UI",
# MAGIC     "init_scripts": [
# MAGIC         {
# MAGIC             "dbfs": {
# MAGIC                 "destination": "dbfs:/FileStore/sedona/sedona-1.2.1-init-master.sh"
# MAGIC             }
# MAGIC         }
# MAGIC     ],
# MAGIC     "runtime_engine": "STANDARD",
# MAGIC     "cluster_id": "0704-014006-t9f0pvs3"
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md _For DBR, install python bindings_
# MAGIC 
# MAGIC > Instead of `%pip install apache-sedona==1.2.0` using WHL built from Sedona master (1.2.1)

# COMMAND ----------

# MAGIC %pip install /dbfs/FileStore/jars/sedona/1.2.1-incubating/apache_sedona-1.2.1-py3-none-any.whl

# COMMAND ----------

import pyspark.sql.functions as f
import mosaic as mos
from sedona.register.geo_registrator import SedonaRegistrator

mos.enable_mosaic(spark, dbutils)       # Enable Mosaic
SedonaRegistrator.registerAll(spark)    # Register Sedona SQL functions

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from sedona.register import SedonaRegistrator

import os

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Sedona functions and types

# COMMAND ----------

SedonaRegistrator.registerAll(spark)

# COMMAND ----------

# MAGIC %scala
# MAGIC // Test for various functions in the catalog
# MAGIC // - rs_add, rs_subtract, rs_divide, and rs_multiply are missing in the Maven 1.2.0 JAR but are in the 1.2.1 from Master.
# MAGIC // - e.g. of one available: "st_polygonfromtext"
# MAGIC println("rs_add",spark.catalog.functionExists("rs_add")) 
# MAGIC println("rs_subtract",spark.catalog.functionExists("rs_subtract"))
# MAGIC println("rs_divide",spark.catalog.functionExists("rs_divide"))
# MAGIC println("rs_multiply",spark.catalog.functionExists("rs_multiply"))

# COMMAND ----------

# MAGIC %md ## Setup Data (1x)

# COMMAND ----------

# Path to directory of geotiff images 
DATA_DIR = "/FileStore/geospatial/sedona-raster"
DATA_DIR_FUSE = "/dbfs" + DATA_DIR

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://github.com/apache/incubator-sedona/blob/master/binder/data/raster/T21HUB_4704_4736_8224_8256.tif?raw=true -O /dbfs/FileStore/geospatial/sedona-raster/T21HUB_4704_4736_8224_8256.tif
# MAGIC wget https://github.com/apache/incubator-sedona/blob/master/binder/data/raster/vya_T21HUB_992_1024_4352_4384.tif?raw=true -O /dbfs/FileStore/geospatial/sedona-raster/vya_T21HUB_992_1024_4352_4384.tif
# MAGIC ls $PWD

# COMMAND ----------

# MAGIC %sh ls /dbfs/FileStore/geospatial/sedona-raster/

# COMMAND ----------

# dbutils.fs.cp("file:/databricks/driver/T21HUB_4704_4736_8224_8256.tif",f"dbfs:{DATA_DIR}/T21HUB_4704_4736_8224_8256.tif")
# dbutils.fs.cp("file:/databricks/driver/vya_T21HUB_992_1024_4352_4384.tif",f"dbfs:{DATA_DIR}/vya_T21HUB_992_1024_4352_4384.tif")
display(dbutils.fs.ls(DATA_DIR))

# COMMAND ----------

# MAGIC %md
# MAGIC # Geotiff Loader 
# MAGIC 
# MAGIC > __Note: if using 1.2.0 JAR (Maven) the spec uses `wkt` instead of `geometry` as name of column (1.2.1 JAR from Master uses `geometry`)__
# MAGIC 
# MAGIC 1. Loader takes as input a path to directory which contains geotiff files or a parth to particular geotiff file
# MAGIC 2. Loader will read geotiff image in a struct named image which contains multiple fields as shown in the schema below which can be extracted using spark SQL

# COMMAND ----------

df_base = spark.read.format("geotiff").option("dropInvalid",True).option("readToCRS", "EPSG:4326").option("disableErrorInCRS", False).load(DATA_DIR)
df_base.printSchema()

# COMMAND ----------

display(df_base.select("image.*").drop("data"))

# COMMAND ----------

df_flat = df_base.selectExpr(
    "image.origin as origin",
    "ST_GeomFromWkt(image.geometry) as Geom",
    "image.height as height", 
    "image.width as width",
    "image.data as data", 
    "image.nBands as bands"
)
display(df_flat)

# COMMAND ----------

# MAGIC %md
# MAGIC # Extract a particular band from geotiff dataframe using RS_GetBand()

# COMMAND ----------

# MAGIC %md
# MAGIC > `RS_GetBand()` will fetch a particular band from given data array which is the concatination of all the bands

# COMMAND ----------

df_band = (
    df_flat.selectExpr(
        "Geom",
        "RS_GetBand(data, 1,bands) as Band1",
        "RS_GetBand(data, 2,bands) as Band2",
        "RS_GetBand(data, 3,bands) as Band3", 
        "RS_GetBand(data, 4,bands) as Band4"
    )
)
df_band.createOrReplaceTempView("allbands")
display(df_band)

# COMMAND ----------

# MAGIC %md
# MAGIC # Map Algebra operations on band values

# COMMAND ----------

# MAGIC %md
# MAGIC > `RS_NormalizedDifference()` can be used to calculate NDVI for a particular geotiff image since it uses same computational formula as ndvi

# COMMAND ----------

df_ND = df_band.selectExpr("RS_NormalizedDifference(Band1, Band2) as normDiff")
display(df_ND)

# COMMAND ----------

# MAGIC %md 
# MAGIC > `RS_Mean()` can be used to calculate mean of pixel values in a particular spatial band

# COMMAND ----------

df_mean = df_band.selectExpr("RS_Mean(Band1) as mean")
display(df_mean)

# COMMAND ----------

# MAGIC %md
# MAGIC > `RS_Mode()` is used to calculate mode in an array of pixels and returns a array of double with size 1 in case of unique mode

# COMMAND ----------

df_mode = df_band.selectExpr("RS_Mode(Band1) as mode")
display(df_mode)

# COMMAND ----------

# MAGIC %md
# MAGIC > `RS_GreaterThan()` is used to mask all the values with 1 which are greater than a particular threshold

# COMMAND ----------

df_gt = spark.sql("Select RS_GreaterThan(Band1,1000.0) as greaterthan from allbands")
display(df_gt.limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC > `RS_GreaterThanEqual()` is used to mask all the values with 1 which are greater than a particular threshold

# COMMAND ----------

df_gte = spark.sql("Select RS_GreaterThanEqual(Band1,360.0) as greaterthanEqual from allbands")
display(df_gte.limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC > `RS_LessThan()` is used to mask all the values with 1 which are less than a particular threshold

# COMMAND ----------

df_lt = spark.sql("Select RS_LessThan(Band1,1000.0) as lessthan from allbands")
display(df_lt.limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC > `RS_LessThanEqual()` is used to mask all the values with 1 which are less than equal to a particular threshold

# COMMAND ----------

df_lte = spark.sql("Select RS_LessThanEqual(Band1,2890.0) as lessthanequal from allbands")
display(df_lte.limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC > `RS_Add()` can add two spatial bands together -- __Not available in 1.2.0 from maven; IS available in 1.2.1 from master.__

# COMMAND ----------

df_sum = df_band.selectExpr("RS_Add(Band1, Band2) as sumOfBand")
display(df_sum.limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC > `RS_Subtract()` can subtract two spatial bands together -- __Not available in 1.2.0 from maven; IS available in 1.2.1 from master.__

# COMMAND ----------

df_subtract = df_band.selectExpr("RS_Subtract(Band1, Band2) as diffOfBand")
display(df_subtract.limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC > `RS_Multiply()` can multiple two bands together -- __Not available in 1.2.0 from maven; IS available in 1.2.1 from master.__

# COMMAND ----------

df_multiply = df_band.selectExpr("RS_Multiply(Band1, Band2) as productOfBand")
display(df_multiply.limit(1))

# COMMAND ----------

# MAGIC %md 
# MAGIC > `RS_Divide()` can divide two bands together -- __Not available in 1.2.0 from maven; IS available in 1.2.1 from master.__

# COMMAND ----------

df_divide = df_band.selectExpr("RS_Divide(Band1, Band2) as divisionOfBand")
display(df_divide.limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC > `RS_MultiplyFactor()` will multiply a factor to a spatial band

# COMMAND ----------

df_mulfac = df_band.selectExpr("RS_MultiplyFactor(Band2, 2) as target")
display(df_mulfac.limit(1))

# COMMAND ----------

# MAGIC %md 
# MAGIC > `RS_BitwiseAND()` will return AND between two values of Bands

# COMMAND ----------

df_bitwiseAND = df_band.selectExpr("RS_BitwiseAND(Band1, Band2) as AND")
display(df_bitwiseAND.limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC > `RS_BitwiseOR()` will return OR between two values of Bands

# COMMAND ----------

df_bitwiseOR = df_band.selectExpr("RS_BitwiseOR(Band1, Band2) as OR")
display(df_bitwiseOR.limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC > `RS_Count()` will calculate the total number of occurence of a target value

# COMMAND ----------

df_count = df_band.selectExpr("RS_Count(RS_GreaterThan(Band1,1000.0), 1.0) as count")
display(df_count)

# COMMAND ----------

# MAGIC %md
# MAGIC > `RS_Modulo()` will calculate the modulus of band value with respect to a given number

# COMMAND ----------

df_modulo = df_band.selectExpr("RS_Modulo(Band1, 21.0) as modulo ")
display(df_modulo.limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC > `RS_SquareRoot()` will calculate calculate square root of all the band values upto two decimal places

# COMMAND ----------

df_root = df_band.selectExpr("RS_SquareRoot(Band1) as root")
display(df_root.limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC > `RS_LogicalDifference()` will return value from band1 if value at that particular location is not equal tp band1 else it will return 0

# COMMAND ----------

df_logDiff = df_band.selectExpr("RS_LogicalDifference(Band1, Band2) as loggDifference")
display(df_logDiff.limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC > `RS_LogicalOver()` will iterate over two bands and return value of first band if it is not equal to 0 else it will return value from later band

# COMMAND ----------

df_logOver = df_band.selectExpr("RS_LogicalOver(Band3, Band2) as logicalOver")
display(df_logOver.limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualising Geotiff Images
# MAGIC 
# MAGIC 1. Normalize the bands in range [0-255] if values are greater than 255
# MAGIC 2. Process image using RS_Base64() which converts in into a base64 string
# MAGIC 3. Embedd results of RS_Base64() in RS_HTML() to embedd into IPython notebook
# MAGIC 4. Process results of RS_HTML() as below:

# COMMAND ----------

# MAGIC %md ### Plotting images as a dataframe using geotiff Dataframe

# COMMAND ----------

df_gtif = (
    spark
        .read
            .format("geotiff")
            .option("dropInvalid",True)
            .option("readToCRS", "EPSG:4326")
        .load(DATA_DIR)
        .selectExpr(
            "image.origin as origin",
            "ST_GeomFromWkt(image.geometry) as Geom",
            "image.height as height", 
            "image.width as width", 
            "image.data as data", 
            "image.nBands as bands"
        )
        .selectExpr(
            "RS_GetBand(data,1,bands) as targetband", 
            "height", "width", "bands", "Geom"
        )
)

# COMMAND ----------

df_base64 = df_gtif.selectExpr(
    "Geom", 
    "RS_Base64(height,width,RS_Normalize(targetBand), RS_Array(height*width,0.0), RS_Array(height*width, 0.0)) as red",
    "RS_Base64(height,width,RS_Array(height*width, 0.0), RS_Normalize(targetBand), RS_Array(height*width, 0.0)) as green",
    "RS_Base64(height,width,RS_Array(height*width, 0.0),  RS_Array(height*width, 0.0), RS_Normalize(targetBand)) as blue",
    "RS_Base64(height,width,RS_Normalize(targetBand), RS_Normalize(targetBand),RS_Normalize(targetBand)) as RGB" 
)

display(
    df_base64
        .selectExpr(
            "Geom","RS_HTML(red) as RedBand",
            "RS_HTML(blue) as BlueBand",
            "RS_HTML(green) as GreenBand", 
            "RS_HTML(RGB) as CombinedBand"
        )
)

# COMMAND ----------

# MAGIC %md > Using `displayHTML` to render in Databricks

# COMMAND ----------

displayHTML(
    df_base64.selectExpr(
        "Geom","RS_HTML(red) as RedBand",
        "RS_HTML(blue) as BlueBand",
        "RS_HTML(green) as GreenBand",
        "RS_HTML(RGB) as CombinedBand")
    .limit(2)
    .toPandas()
    .to_html(escape=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing GeoTiff Images

# COMMAND ----------

user_str = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("user").get()
print(f"user_str? '{user_str}'")
#print(f"""files --> {dbutils.fs.ls(f"/home/{user_str}")}""")

# COMMAND ----------

SAVE_DIR = f"/home/{user_str}/geospatial/sedona/raster-out"
SAVE_DIR_FUSE = "/dbfs" + SAVE_DIR

# COMMAND ----------

# MAGIC %md
# MAGIC > Writing GeoTiff DataFrame as GeoTiff Images

# COMMAND ----------

df_save = (
    spark.read.format("geotiff").option("dropInvalid",True).option("readToCRS", "EPSG:4326").load(DATA_DIR)
    .selectExpr(
        "image.origin as origin",
        "ST_GeomFromWkt(image.geometry) as Geom",
        "image.height as height",
        "image.width as width",
        "image.data as data",
        "image.nBands as bands"
    )
)

# COMMAND ----------

df_save.write.mode("overwrite").format("geotiff").option("writeToCRS", "EPSG:4326").option("fieldGeometry", "Geom").option("fieldNBands", "bands").save(SAVE_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC > Writing GeoTiff Images in a Single Partition

# COMMAND ----------

(
    df_save.coalesce(1)
        .write
            .mode("overwrite")
            .format("geotiff")
            .option("writeToCRS", "EPSG:4326")
            .option("fieldGeometry", "Geom")
            .option("fieldNBands", "bands")
    .save(SAVE_DIR)
)

# COMMAND ----------

# MAGIC %md
# MAGIC > Find the Partition of the Written GeoTiff Images. If you did not write with coalesce(1), change the below code to adjust the writtenPath.

# COMMAND ----------

import os 
writtenPath = SAVE_DIR_FUSE
dirList = os.listdir(writtenPath)
for item in dirList:
    if os.path.isdir(writtenPath + "/" + item):
        writtenPath += "/" + item
        break
        
writtenPath = writtenPath[5:] # MLJ: Modify to remove the FUSE portion (aka '/dbfs')
display(dbutils.fs.ls(writtenPath))

# COMMAND ----------

# MAGIC %md
# MAGIC > Load and Visualize Written GeoTiff Image.

# COMMAND ----------

df_load = (
    spark.read.format("geotiff").option("dropInvalid",True).option("readToCRS", "EPSG:4326").load(writtenPath)
    .selectExpr(
        "image.origin as origin",
        "ST_GeomFromWkt(image.geometry) as Geom",
        "image.height as height", "image.width as width", "image.data as data", "image.nBands as bands")
    .selectExpr(
        "RS_GetBand(data,1,bands) as targetband",
        "height", "width", "bands", "Geom")
)

# COMMAND ----------

df_base64 = (
    df_load.selectExpr(
    "Geom", 
    "RS_Base64(height,width,RS_Normalize(targetBand), RS_Array(height*width,0.0), RS_Array(height*width, 0.0)) as red",
    "RS_Base64(height,width,RS_Array(height*width, 0.0), RS_Normalize(targetBand), RS_Array(height*width, 0.0)) as green",
    "RS_Base64(height,width,RS_Array(height*width, 0.0),  RS_Array(height*width, 0.0), RS_Normalize(targetBand)) as blue",
    "RS_Base64(height,width,RS_Normalize(targetBand), RS_Normalize(targetBand),RS_Normalize(targetBand)) as RGB" 
    )
    .selectExpr(
        "Geom","RS_HTML(red) as RedBand","RS_HTML(blue) as BlueBand","RS_HTML(green) as GreenBand", "RS_HTML(RGB) as CombinedBand"
    )
)
displayHTML(df_base64.limit(2).toPandas().to_html(escape=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation of GeoTiff Images

# COMMAND ----------

# MAGIC %md
# MAGIC > First load GeoTiff Images

# COMMAND ----------

df_t = (
    spark
    .read
        .format("geotiff")
        .option("dropInvalid",True)
        .option("readToCRS", "EPSG:4326")
        .option("disableErrorInCRS", False)
    .load(DATA_DIR)
    .selectExpr(
        "image.origin as origin",
        "ST_GeomFromWkt(image.geometry) as geom", 
        "image.height as height", 
        "image.width as width", 
        "image.data as data", 
        "image.nBands as bands"
    )
)
display(df_t)

# COMMAND ----------

# MAGIC %md
# MAGIC >  First extract the bands for which normalized difference index needs to be calculated

# COMMAND ----------

df_t1 = (
    df_t.selectExpr(
        "origin", 
        "geom",
        "width", 
        "height", 
        "data", 
        "bands", 
        "RS_GetBand(data, 1, bands) as band1", 
        "RS_GetBand(data, 2, bands) as band2"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > Get the normalized difference index between the extracted bands

# COMMAND ----------

df_nd = (
    df_t1.selectExpr(
        "origin", 
        "geom",
        "width", 
        "height", 
        "data", 
        "bands", 
        "RS_NormalizedDifference(band2, band1) as normalizedDifference"
    )
)
display(df_nd)

# COMMAND ----------

# MAGIC %md > `RS_Append()` takes the data array containing bands, a new band to be appended, and number of total bands in the data array. It appends the new band to the end of the data array and returns the appended data-- __Not available in 1.2.0 from maven; IS available in 1.2.1 from master.__

# COMMAND ----------

df_append = (
    df_nd.selectExpr(
        "origin", "geom", 
        "RS_Append(data, normalizedDifference, bands) as data_edited", 
        "height", "width", "bands"
    )
    .drop("data")
    .withColumn("nBand_edited", col("bands") + 1)
    .drop("bands")
)
display(df_append)

# COMMAND ----------

# MAGIC %md _Could write GeoTiff DataFrame as GeoTiff image similar as before_
# MAGIC 
# MAGIC Example
# MAGIC 
# MAGIC ```
# MAGIC (
# MAGIC df_append.coalesce(1)
# MAGIC   .write.mode("overwrite").format("geotiff")
# MAGIC     .option("writeToCRS", "EPSG:4326")
# MAGIC     .option("fieldGeometry", "geom")
# MAGIC     .option("fieldNBands", "nBand_edited")
# MAGIC     .option("fieldData", "data_edited")
# MAGIC   .save(SAVE_DIR)
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## User can also create some UDF manually to manipulate Geotiff dataframes

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > Sample UDF calculates sum of all the values in a band which are greater than 1000.0

# COMMAND ----------

def SumOfValues(band):
    total = 0.0
    for num in band:
        if num>1000.0:
            total+=1
    return total

df_u1 = (
    spark.read.format("geotiff").option("dropInvalid",True).option("readToCRS", "EPSG:4326").load(DATA_DIR)
    .selectExpr("image.origin as origin","ST_GeomFromWkt(image.geometry) as Geom", 
                "image.height as height", "image.width as width", "image.data as data", "image.nBands as bands")
    .selectExpr("RS_GetBand(data,1,bands) as targetband", 
                "height", "width", "bands", "Geom")
)
    
calculateSum = udf(SumOfValues, DoubleType())
spark.udf.register("RS_Sum", calculateSum)

df_sum = df_u1.selectExpr("RS_Sum(targetband) as sum")
display(df_sum)

# COMMAND ----------

# MAGIC %md > Sample UDF to visualize a particular region of a GeoTiff image

# COMMAND ----------

def generatemask(band, width,height):
    for (i,val) in enumerate(band):
        if (i%width>=12 and i%width<26) and (i%height>=12 and i%height<26):
            band[i] = 255.0
        else:
            band[i] = 0.0
    return band

maskValues = udf(generatemask, F.ArrayType(DoubleType()))
spark.udf.register("RS_MaskValues", maskValues)

df_u1b64 = (
    df_u1.selectExpr(
        "Geom",
        "RS_Base64(height,width,RS_Normalize(targetband), RS_Array(height*width,0.0), RS_Array(height*width, 0.0), RS_MaskValues(targetband,width,height)) as region" 
    )
    .selectExpr("Geom","RS_HTML(region) as selectedregion")
)

displayHTML(df_u1b64.limit(2).toPandas().to_html(escape=False))

# COMMAND ----------


