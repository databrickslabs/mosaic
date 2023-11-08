# Databricks notebook source
# MAGIC %md
# MAGIC ## Install the libraries and prepare the environment

# COMMAND ----------

# MAGIC %pip install databricks-mosaic rasterio==1.3.5 --quiet gdal==3.4.3 pystac pystac_client planetary_computer tenacity rich pandas==1.5.3 supermercado mercantile shapely

# COMMAND ----------

# MAGIC %md
# MAGIC Set up config parameters.

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "400")

# COMMAND ----------

import library
import mosaic as mos
import rasterio

from io import BytesIO
from matplotlib import pyplot
from rasterio.io import MemoryFile
from pyspark.sql import functions as F

mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

# COMMAND ----------

# MAGIC %reload_ext autoreload
# MAGIC %autoreload 2
# MAGIC %reload_ext library

# COMMAND ----------

# MAGIC %sql
# MAGIC USE odin_alaska;
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC Load a single band data set.
# MAGIC We use 'rst_to_overlapping_tiles' to generate minimal overalapping between tiles. This is a better approach than subdivide when we need to reproject rasters. 'F.lit(5)' represents the % of overlap between neighbour tiles. We need to initialise noData values as well since Sentinel doesnt always set them correctly.
# MAGIC

# COMMAND ----------

tiles_df = (
  spark.read
    .format("gdal")
    .option("raster_storage", "in-memory")
    .load("dbfs:/FileStore/geospatial/odin/alaska/B02/")
    .repartition(400)
    .withColumn("tile", mos.rst_to_overlapping_tiles("tile", F.lit(1000), F.lit(1000), F.lit(5)))
    .withColumn("tile", mos.rst_initnodata("tile"))
    .withColumn("bbox", mos.st_astext(mos.rst_boundingbox("tile")))
    .withColumn("bbox4326", mos.st_updatesrid("bbox", "srid", F.lit(4326)))
)

# COMMAND ----------

tiles_df.limit(5).display()

# COMMAND ----------

to_display = tiles_df.select("bbox4326")

# COMMAND ----------

# MAGIC %md
# MAGIC We will check the bboxes of our mini tiles now, notice they should overlap by 5% since we used the overalpping tiles.

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC to_display bbox4326 geometry

# COMMAND ----------

# MAGIC %md
# MAGIC We define 2 udfs for handling tessellation in mercantile tiles. The first ones will generate a tile set based on the bbox of the input raster tile. The second one will generate an xyz id in "z/x/y" format as some of the web services use it. This is an arbitrary format choice, you can change that if you want to. Notice both functions work on a single zoom, and so does the whole script. I would not advise mixing zooms in a single pipeline, but separate it inot separate flows.

# COMMAND ----------

from shapely.geometry import shape
import shapely.wkt
import mercantile as mc

@udf("array<string>")
def xyz_tesselate(geom_wkt, zoom):
  geom = shapely.wkt.loads(geom_wkt)
  bounds = geom.bounds
  tiles = list(mc.tiles(bounds[0], bounds[1], bounds[2], bounds[3], zooms=[zoom]))
  return [shape(mc.feature(t)["geometry"]).wkt for t in tiles]

@udf("string")
def xyz_id(geom_wkt, zoom):
  geom = shapely.wkt.loads(geom_wkt)
  cent = geom.centroid
  tile = mc.tile(cent.x, cent.y, zoom)
  return f"{tile.z}/{tile.x}/{tile.y}"

# COMMAND ----------

tessellatted_tiles = tiles_df\
  .withColumn("id", F.monotonically_increasing_id())\
  .withColumn("xyz_tile", xyz_tesselate("bbox4326", F.lit(11)))\
  .withColumn("xyz_tile", F.explode("xyz_tile"))\
  .withColumn("xyz_id", xyz_id("xyz_tile", F.lit(11)))

# COMMAND ----------

# MAGIC %md
# MAGIC We can display our tessellation examples (these are partial since we randomly sample the inputs.)

# COMMAND ----------

to_display = tessellatted_tiles.select("bbox4326", "xyz_tile")

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC to_display bbox4326 geometry 10000

# COMMAND ----------

to_display = tessellatted_tiles\
  .limit(10)\
  .withColumn("clip_region", mos.st_updatesrid("xyz_tile", F.lit(4326), "srid"))\
  .select(
    "tile",
    mos.rst_clip("tile", "clip_region").alias("mini_tile")
  ).where(
    ~mos.rst_isempty("mini_tile")
  ).collect()

# COMMAND ----------

library.plot_raster(to_display[0]["tile"]["raster"])

# COMMAND ----------

library.plot_raster(to_display[4]["mini_tile"]["raster"])

# COMMAND ----------

# MAGIC %md
# MAGIC We need a scala udf to reproject the raster to 3857. This will be made available in a new version of mosaic, so for now is just a temp solution.

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.labs.mosaic.core.raster.operator.proj._
# MAGIC import org.apache.spark.sql.functions.udf
# MAGIC import com.databricks.labs.mosaic.core.raster.api.GDAL
# MAGIC import org.gdal.osr.SpatialReference
# MAGIC import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
# MAGIC import com.databricks.labs.mosaic.core.raster.io.RasterCleaner
# MAGIC
# MAGIC def reprojectRaster(tile: Array[Byte], driver: String, crsID: Int): Array[Byte] = {
# MAGIC
# MAGIC   val raster = GDAL.raster(tile, "", driver)
# MAGIC
# MAGIC   val destSR = new SpatialReference()
# MAGIC   destSR.ImportFromEPSG(crsID)
# MAGIC   destSR.SetAxisMappingStrategy(org.gdal.osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
# MAGIC
# MAGIC   val rstRes = RasterProject.project(raster, destSR)
# MAGIC   val tileRes = new MosaicRasterTile(null, rstRes, "", rstRes.getDriversShortName)
# MAGIC
# MAGIC   val encoded = tileRes.serialize()
# MAGIC
# MAGIC   RasterCleaner.dispose(raster)
# MAGIC   RasterCleaner.dispose(tileRes)
# MAGIC
# MAGIC   encoded.getBinary(1)
# MAGIC   
# MAGIC }
# MAGIC
# MAGIC
# MAGIC val reprojectRasterUDF = udf((tile: Array[Byte], driver: String, crsID: Int) => reprojectRaster(tile, driver, crsID))
# MAGIC spark.udf.register("reprojectRaster", reprojectRasterUDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Here we will do the clipping against xyz tiles, for that we need them to be projected to raster SRID. They are originally generated in 4326 (line 3). Then we will clip the raster with the geometry we projected (line 4). The we will filter out empty pieces to save processing time when merging fragments of tiles. The first group by is to resolve overlaps and for that we use rst_merge_agg, the secong group by is to average across information coming from different images if there were overalps in the raw Sentinel data, this uses aritmetic average and it is implemented through rst_combineavg_agg. Finally we project the tiles to 3857 and we project bounding box of xyz tile to 3857.

# COMMAND ----------

result = tessellatted_tiles\
  .limit(1000)\
  .withColumn("clip_region", mos.st_updatesrid("xyz_tile", F.lit(4326), "srid"))\
  .withColumn("tile", mos.rst_clip("tile", "clip_region"))\
  .where(~mos.rst_isempty("tile"))\
  .groupBy("xyz_id", "tile.parentPath")\
  .agg(
    mos.rst_merge_agg("tile").alias("tile"),
    F.first("xyz_tile").alias("xyz_tile")
  )\
  .groupBy("xyz_id")\
  .agg(
    mos.rst_combineavg_agg("tile").alias("tile"),
    F.first("xyz_tile").alias("xyz_tile")
  )\
  .withColumn("result_tile", F.expr("reprojectRaster(tile.raster, 'GTiff', 3857)"))\
  .withColumn("xyz_tile", mos.st_updatesrid("xyz_tile", F.lit(4326), F.lit(3857)))\
  .collect()

# COMMAND ----------

library.plot_raster(result[0]["result_tile"])

# COMMAND ----------

library.plot_raster(result[0]["tile"]["raster"])

# COMMAND ----------

# MAGIC %md 
# MAGIC Please note that the last udf only operates on raster individually without the tile wrapper.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC You can write the results to a delta table or if you need individual files you can create a udf that just writes out raster binaries using with file open pattern inside a python udf.
