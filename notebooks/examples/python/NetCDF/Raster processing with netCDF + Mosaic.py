# Databricks notebook source
# MAGIC %md # Raster processing with netCDF + Mosaic
# MAGIC 
# MAGIC **Prereqs:**
# MAGIC 
# MAGIC * Install `netCDF4` from PyPI
# MAGIC * Install `org.locationtech.jts:jts-io:1.19.0` from Maven
# MAGIC * Install `mosaic-0.33` from [Project Mosaic (Release v0.3.3)](https://github.com/databrickslabs/mosaic/releases/tag/v0.3.3)
# MAGIC 
# MAGIC **Notes:**
# MAGIC 
# MAGIC * Requires [numpy](http://numpy.scipy.org) and netCDF/HDF5 C libraries.
# MAGIC * Github site: https://github.com/Unidata/netcdf4-python
# MAGIC * Online docs: http://unidata.github.io/netcdf4-python/
# MAGIC * Based on Konrad Hinsen's old [Scientific.IO.NetCDF](http://dirac.cnrs-orleans.fr/plone/software/scientificpython/) API, with lots of added netcdf version 4 features.
# MAGIC * Developed by Jeff Whitaker at NOAA, with many contributions from users.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Part I - Ingest netCDF in Parallel to Spark Dataframe

# COMMAND ----------

import netCDF4
import numpy as np

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Inspect netcdf file with driver code

# COMMAND ----------

f = netCDF4.Dataset('/dbfs/ml/blogs/geospatial/data/netcdf/tables/SkyWise_CONUS_SurfaceAnalysis_Daily_20190220_000000-f4048.nc')
print(f)

# COMMAND ----------

lat, lon, temp = f.variables['latitude'], f.variables['longitude'], f.variables['maximum_temperature']
print(lat)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Get a parameter list - in this case, a list of ~20 files (replace this with thousands of files)

# COMMAND ----------

file_list = []
date_file_list = dbutils.fs.ls("/ml/blogs/geospatial/data/netcdf/tables")
for fi in date_file_list:
  sub_list = dbutils.fs.ls(fi.path)
  file_list.append(sub_list[0].path.replace("dbfs:", "/dbfs"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Test out Single-threaded transformation

# COMMAND ----------

import pandas as pd
from itertools import product

def get_temp(filename):
  current_file = filename
  f = netCDF4.Dataset(current_file)
  lat, lon, temp = f.variables['latitude'], f.variables['longitude'], f.variables['maximum_temperature']

  ydim = len(lon[0,:])
  xdim = len(lat[:,0])

  new_temp = temp[:].reshape([ydim*xdim, 1])
  prod = product(lat[:,0], lon[0,:])
  df = pd.DataFrame(prod, columns=['lat', 'lon'])
  df['temp'] = new_temp
  f.close()
  return(df)

  
output_df = get_temp('/dbfs/ml/blogs/geospatial/data/netcdf/tables/SkyWise_CONUS_SurfaceAnalysis_Daily_20190220_000000-f4048.nc')
output_df.head()

# COMMAND ----------

output_schema = spark.createDataFrame(output_df).schema
output_schema

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Write and Run Pandas UDF to coalesce File Info Into Spark Data Frame

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.functions import PandasUDFType
from pyspark.sql.types import *

sdf =  spark.createDataFrame(sc.parallelize(file_list), StringType())

@pandas_udf(output_schema, functionType=PandasUDFType.GROUPED_MAP)  
def p_get_temp(filename):
  current_file = filename.iloc[0]['value']
  print('current_file type is', type(current_file))
  print('current_file is: ', current_file)
  f = netCDF4.Dataset(current_file)
  lat, lon, temp = f.variables['latitude'], f.variables['longitude'], f.variables['maximum_temperature']

  ydim = len(lon[0,:])
  xdim = len(lat[:,0])

  new_temp = temp[:].reshape([ydim*xdim, 1])
  prod = product(lat[:,0], lon[0,:])
  df = pd.DataFrame(prod, columns=['lat', 'lon'])
  df['temp'] = new_temp
  f.close()
  return(df)

output_df = sdf.groupBy(col("value")).apply(p_get_temp)

# COMMAND ----------

display(output_df)

# COMMAND ----------

output_df.createOrReplaceTempView("rp_weather_netcdf_delta")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Part II - Add Geospatial Index for Large-Scale Geospatial Analytics

# COMMAND ----------

# MAGIC %sh 
# MAGIC 
# MAGIC sudo apt-get install -y cmake

# COMMAND ----------

# MAGIC %scala 
# MAGIC 
# MAGIC // UDFs for comparison with Mosaic
# MAGIC // using JTS + H3 directly 
# MAGIC 
# MAGIC import com.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}
# MAGIC import com.uber.h3core.H3Core;
# MAGIC import com.uber.h3core.util.GeoCoord;
# MAGIC import scala.collection.JavaConversions._
# MAGIC import scala.collection.mutable.ListBuffer
# MAGIC import scala.collection.JavaConverters._
# MAGIC 
# MAGIC object H3 extends Serializable {
# MAGIC   private val _instance = H3Core.newInstance();
# MAGIC   def instance() = _instance
# MAGIC }
# MAGIC 
# MAGIC def geoToH3 = udf((latitude:Double, longitude:Double, resolution:Int)=>{
# MAGIC   //h3.instance.geoToH3Address(latitude, longitude, resolution); //--> Long.toHexString(long h3)
# MAGIC   H3.instance.geoToH3(latitude, longitude, resolution)
# MAGIC })
# MAGIC 
# MAGIC def polygonToH3 = udf((geometry: Geometry, resolution: Int)=>{
# MAGIC   var points: java.util.List[com.uber.h3core.util.GeoCoord] = List();
# MAGIC   var holes: java.util.List[java.util.List[com.uber.h3core.util.GeoCoord]] = List();
# MAGIC   if (geometry.getGeometryType == "Polygon"){
# MAGIC     points = ListBuffer(geometry.getCoordinates().toList.map(coord => new GeoCoord(coord.y,coord.x)) : _*);  
# MAGIC   }
# MAGIC   asScalaBuffer(H3.instance.polyfill(points, holes ,resolution)).toList
# MAGIC });
# MAGIC 
# MAGIC def multiPolygonToH3 = udf((geometry: Geometry, resolution: Int)=>{
# MAGIC   var points: java.util.List[com.uber.h3core.util.GeoCoord] = List();
# MAGIC   var holes: java.util.List[java.util.List[com.uber.h3core.util.GeoCoord]] = List();
# MAGIC   if (geometry.getGeometryType == "MultiPolygon"){  
# MAGIC     val numGeometries = geometry.getNumGeometries();
# MAGIC     if (numGeometries > 0){
# MAGIC       points = ListBuffer(geometry.getGeometryN(0).getCoordinates().toList.map(coord => new GeoCoord(coord.y,coord.x)): _*);  
# MAGIC     }
# MAGIC     if (numGeometries >1){
# MAGIC       holes = ListBuffer((1 to (numGeometries-1)).toList.map(n => {
# MAGIC         val templist:  java.util.List[com.uber.h3core.util.GeoCoord] = ListBuffer(geometry.getGeometryN(n).getCoordinates().toList.map(coord => new GeoCoord(coord.y,coord.x)): _*)
# MAGIC         templist
# MAGIC       }): _*);
# MAGIC     }
# MAGIC   }
# MAGIC   asScalaBuffer(H3.instance.polyfill(points, holes ,resolution)).toList
# MAGIC });

# COMMAND ----------

# MAGIC %python 
# MAGIC 
# MAGIC geoToH3 = udf(geo_to_h3)

# COMMAND ----------

# MAGIC %scala 
# MAGIC 
# MAGIC import org.apache.spark.sql.functions._ 
# MAGIC 
# MAGIC val res = 8
# MAGIC val weather_df = spark.sql("select temp, lat, lon from rp_weather_netcdf_delta").withColumn("h3_index", geoToH3(col("lat"), col("lon"), lit(res)))
# MAGIC weather_df.write.mode("overwrite").format("delta").saveAsTable("rp_weather_netcdf_delta_silver2")

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC OPTIMIZE RP_WEATHER_NETCDF_DELTA_SILVER2 
# MAGIC ZORDER BY H3_INDEX

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### For that Home Depot in Antarctica, What is the Temperature? 
# MAGIC 
# MAGIC <img src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSetAA5zofmNPSq8Axq6zRu1klfw6eDODQDsO75s8JbIiqwVAE3Bw&s" width = 500>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC As long as we set the resolution large enough, our hexagon will refer to a region where we can report the temperature for Home Depot.

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select * from RP_WEATHER_NETCDF_DELTA_SILVER2

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val res = 8
# MAGIC val home_depot_lat_long = Seq((31, -100.80))
# MAGIC 
# MAGIC val h3_lat_range = home_depot_lat_long.toDF("lat", "lon").withColumn("hd_h3_idx", geoToH3(col("lat"), col("lon"), lit(res)))
# MAGIC 
# MAGIC display(h3_lat_range)

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select * from RP_WEATHER_NETCDF_DELTA_SILVER2
# MAGIC where h3_index = 613765671894908927

# COMMAND ----------

@pandas_udf(output_schema, functionType=PandasUDFType.GROUPED_MAP)  
def p_get_raster(filename):
  current_file = filename.iloc[0]['value']
  print('current_file type is', type(current_file))
  print('current_file is: ', current_file)
  f = netCDF4.Dataset(current_file)
  raster_data = netCDF4.raster(f)
  lat, lon, temp = f.variables['latitude'], f.variables['longitude'], f.variables['maximum_temperature']

  ydim = len(lon[0,:])
  xdim = len(lat[:,0])

  new_temp = temp[:].reshape([ydim*xdim, 1])
  prod = product(lat[:,0], lon[0,:])
  df = pd.DataFrame(prod, columns=['lat', 'lon'])
  df['raster'] = raster_data
  df['temp'] = new_temp
  f.close()
  return(df)

output_df = sdf.groupBy(col("value")).apply(p_get_raster)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Extract the CRS (coordinate reference system) associated with the dataset identifying where the raster is located in geographic space. 

# COMMAND ----------

@udf(returnType=StringType()) 
def get_crs(content):
  # Read the in-memory tiff file
  with MemoryFile(bytes(content)) as memfile:
    with memfile.open() as data:
      # Use netcdf with the data object
      return str(data.crs)

df_bin.withColumn("crs", get_crs("content")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract masks from images
# MAGIC An image mask identifies the regions of the image where there is valid data to be processed.

# COMMAND ----------

from pyspark.sql.types import ArrayType, StringType

@udf(returnType=ArrayType(StringType())) 
def get_mask_shapes(content):
  geometries = []
  
  # Read the in-memory tiff file
  with MemoryFile(bytes(content)) as memfile:
    with memfile.open() as data:

      # Read the dataset's valid data mask as a ndarray.
      mask = data.dataset_mask()

      # Extract feature shapes and values from the array.
      for geom, val in nc.features.shapes(
              mask, transform=data.transform):

        if val > 0: # Only append shapes that have a positive maks value
          
          # Transform shapes from the dataset's own coordinate
          # reference system to CRS84 (EPSG:4326).
          geom = nc.warp.transform_geom(
              data.crs, 'EPSG:4326', geom, precision=6)

          geometries.append(json.dumps(geom))
          
  return geometries


# COMMAND ----------

df_masks = (df_bin
            .withColumn("mask_json_shapes", get_mask_shapes("content"))
            .withColumn("mask_json", explode("mask_json_shapes"))
            # Convert geoJSON to WKB
            .withColumn("mask_wkb", mos.st_aswkb(mos.st_geomfromgeojson("mask_json")))
            .drop("content", "mask_json_shapes", "mask_json")
            .cache() # Caching while developing, TODO: Remove in prod
           )
df_masks.display()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_masks "mask_wkb" "geometry"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tessellate with Mosaic

# COMMAND ----------

df_chips = (df_masks
            # Tessellate with Mosaic
            .withColumn("chips", mos.grid_tessellateexplode("mask_wkb", lit(h3_resolution)))
            .select("path", "modificationTime", "chips.*")
            .withColumn("chip_geojson", mos.st_asgeojson("wkb"))
            .cache() # TODO remove for distribution
           )
df_chips.display()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_chips "wkb" "geometry" 10000

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pixels to chips

# COMMAND ----------

import pyspark.sql.functions as F
import numpy as np
from pyspark.sql.types import ArrayType, StringType, DoubleType, StructType, StructField, LongType, IntegerType
import nc.mask

schema = ArrayType(
  StructType([
    StructField("values", ArrayType(DoubleType())),
    StructField("nonzero_pixel_count", IntegerType()),
  ]))

@udf(returnType=schema)
def get_shapes_avg(content, chips):
  chip_values = []
  
  # Read the in-memory tiff file
  with MemoryFile(bytes(content)) as memfile:
    with memfile.open() as data:
      
      for chip in chips:
        chip_geojson = json.loads(chip)                                        # Chip in GeoJSON format
        geom = nc.warp.transform_geom('EPSG:4326', data.crs, chip_geojson, precision=6)  # Project chips to the image CRS
        out_image, out_transform = nc.mask.mask(data, [geom], crop=True, filled=False)   # Crop the image on a shape containing the chip
        
        val = np.average(out_image, axis=(1,2)).tolist() # Aggregated by band
        nonzeroes = np.count_nonzero(out_image.mask)     # Cound pixels within the chip shape
        
        chip_values.append({
          "values": val,                     # Aggregated pixel values by band
          "nonzero_pixel_count": nonzeroes   # Number of pixels within the mask shape
        })
        
  return chip_values

df_chipped = (df_chips
              .groupBy("path", "modificationTime")
              .agg(F.collect_list(F.struct("chip_geojson", "index_id", "wkb", "is_core")).alias("chips"))  # Collecting the list of chips
              .join(df_bin, ["path", "modificationTime"])                                # Join with the original data files
              .withColumn("chip_values", get_shapes_avg(col("content"), F.expr("chips.chip_geojson")))             # Execute UDF to aggregate pixels for each chip
              .withColumn("zipped_chip_values", F.arrays_zip("chips", "chip_values"))
              .withColumn("zipped_chip_value", F.explode("zipped_chip_values"))                       # Explode result array in multiple rows
              .select(                                                                   # Select only relevant columns
                col("path"), 
                col("modificationTime"), 
                F.expr("zipped_chip_value.chips.*"),
                F.expr("zipped_chip_value.chip_values.*"),
#                 col("chip.index_id").cast("long").alias("index_id"),
#                 col("chip.nonzero_pixel_count").alias("nonzero_pixel_count"),
                F.expr("zipped_chip_value.chip_values.values[0]").alias("value_band_0")
              )
              .cache()  # TODO: Remove in production
             )
df_chipped.display()

# COMMAND ----------

import pyspark.sql.functions as F

from pyspark.sql.types import ArrayType, StringType, DoubleType, StructType, StructField, LongType, IntegerType

schema = ArrayType(
  StructType([
    StructField("index_id", LongType()), 
    StructField("values", ArrayType(DoubleType())),
    StructField("nonzero_pixel_count", IntegerType()),
  ]))

@udf(returnType=schema)
def get_shapes_avg(content, chips):
  chip_values = []
  
  # Read the in-memory tiff file
  with MemoryFile(bytes(content)) as memfile:
    with memfile.open() as data:
      
      for chip in chips:
        chip_geojson = json.loads(chip["chip_geojson"])                                        # Chip in GeoJSON format
        geom = nc.warp.transform_geom('EPSG:4326', data.crs, chip_geojson, precision=6)  # Project chips to the image CRS
        out_image, out_transform = nc.mask.mask(data, shapes, crop=True, filled=False)   # Crop the image on a shape containing the chip
        
        val = np.average(out_image, axis=(1,2)).tolist() # Aggregated by band
        nonzeroes = np.count_nonzero(out_image.mask)     # Cound pixels within the chip shape
        
        chip_values.append({
          "index_id": chip["index_id"],      # H3 index ID
          "values": val,                     # Aggregated pixel values by band
          "nonzero_pixel_count": nonzeroes   # Number of pixels within the mask shape
        })
        
  return chip_values

df_chipped = (df_chips
              .groupBy("path", "modificationTime")
              .agg(F.collect_list(F.struct("chip_geojson", "index_id", "wkb", "is_core")).alias("chips"))  # Collecting the list of chips
              .join(df_bin, ["path", "modificationTime"])                                # Join with the original data files
              .withColumn("chip_values", get_shapes_avg("content", "chips"))             # Execute UDF to aggregate pixels for each chip
              .withColumn("chip", F.explode("chip_values"))                              # Explode result array in multiple rows
              .select(                                                                   # Select only relevant columns
                col("path"), 
                col("modificationTime"), 
                col("chip.index_id").cast("long").alias("index_id"),
                col("chip.nonzero_pixel_count").alias("nonzero_pixel_count"),
                F.expr("chip.values[0]").alias("value_band_0")
              )
              .cache()  # TODO: Remove in production
             )
df_chipped.display()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_chipped "wkb" "geometry" 10000

# COMMAND ----------

# MAGIC %md
# MAGIC In the Raster TIFF file the shape is the rectangle that contains image (we can get this from <a href="https://rasterio.readthedocs.io/en/latest/quickstart.html#dataset-georeferencing">bounds</a>). We need to cut the image with the same technique, but on top of that we need find all the pixels that fall within each cell and run an aggregation for those pixels (min/max/average/median/etc.).
# MAGIC In order to do that we need to use functions like <a href="https://rasterio.readthedocs.io/en/latest/topics/masking-by-shapefile.html">masking</a> or <a hrefe="https://rasterio.readthedocs.io/en/latest/topics/features.html#burning-shapes-into-a-raster">rasterize</a> to get the portion of the image that corresponds to each grid cell.
# MAGIC This will generate an aggregated information based on the grid cells. We can store that in a table and visualise it, join it with tesselated vectors etc.

# COMMAND ----------

# MAGIC %md ## Join data - environmental exposure

# COMMAND ----------

# Path to directory of the csv for impact assessment
EXP_FILE = "California_Fire_Perimeters.csv"
exposure_fire = spark.read.format("csv").option("header","true").load(DATA_DIR+"/"+EXP_FILE)

# COMMAND ----------

display(exposure_fire)

# COMMAND ----------

exposure_fire.count()

# COMMAND ----------

exposure_fire_tf = (
  exposure_fire
    .drop("ID")
    .withColumn("latitude",col("latitude").cast(DoubleType()))
    .withColumn("longitude",col("longitude").cast(DoubleType()))                                              
    .withColumn("geom", mos.st_astext(mos.st_point(col("longitude"), col("latitude")))) # First we need to creating a new Mosaic Point geometry, and afterwards translate a geometry into its Well-known Text (WKT) representation
)

# COMMAND ----------

# MAGIC %md
# MAGIC We can use Mosaic functionality to identify how to best index our data based on the data inside the specific dataframe. </br>
# MAGIC Selecting an appropriate indexing resolution can have a considerable impact on the performance. </br>

# COMMAND ----------

firesWithIndex = (exppsure_fire_tf
  .withColumn("index_id", mos.grid_pointascellid(col("geom"), lit(h3_resolution)))
)

# COMMAND ----------

display(firesWithIndex)

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC firesWithIndex "geom" "geometry" 100000

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Aggregation

# COMMAND ----------

withFirePerimeter = (
  firesWithIndex.join(
    df_chipped,
    on="index_id",
    how="right")
    .where(
      # If the borough is a core chip (the chip is fully contained within the geometry), then we do not need
      # to perform any intersection, because any point matching the same index will certainly be contained in
      # the borough. Otherwise we need to perform an st_contains operation on the chip geometry.
      col("is_core") | mos.st_contains(col("wkb"), col("geom")))
    .groupBy(["index_id", "wkb", "value_band_0", "nonzero_pixel_count", "is_core"])
    .agg(F.count("geom").alias("point_count"))
    .cache() # TODO: Remove in production
#     drop("count")
)

display(withFirePerimeter)

# COMMAND ----------

withFirePerimeter.count()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC withFirePerimeter "wkb" "geometry" 100000

# COMMAND ----------

# MAGIC %md ### ```ST_Contains``` of Raster and exposure data

# COMMAND ----------

withFirePerimeterExposure = (
  firesWithIndex.join(
    df_chipped,
    on="index_id",
    how="inner")
    .where(
      # If the borough is a core chip (the chip is fully contained within the geometry), then we do not need
      # to perform any intersection, because any point matching the same index will certainly be contained in
      # the borough. Otherwise we need to perform an st_contains operation on the chip geometry.
      col("is_core") | mos.st_contains(col("wkb"), col("geom")))
    .groupBy(["index_id", "wkb", "value_band_0", "nonzero_pixel_count", "is_core"])
    .agg(F.count("geom").alias("point_count"))
    .cache() # TODO: Remove in production
#     drop("count")
)

display(withFirePerimeterExposure)

# COMMAND ----------

display(withFirePremeterExposure)

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC withFirePerimeterExposure "wkb" "geometry" 100000
