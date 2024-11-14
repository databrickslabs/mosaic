package com.databricks.labs

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.datasource.multiread.MosaicDataFrameReader
import org.apache.spark.sql.SparkSession

//noinspection ScalaWeakerAccess
package object mosaic {

    val JTS: GeometryAPI = mosaic.core.geometry.api.JTS
    val H3: IndexSystem = mosaic.core.index.H3IndexSystem
    val BNG: IndexSystem = mosaic.core.index.BNGIndexSystem

    val DATABRICKS_SQL_FUNCTIONS_MODULE = "com.databricks.sql.functions"
    val SPARK_DATABRICKS_GEO_H3_ENABLED = "spark.databricks.geo.h3.enabled"

    val MOSAIC_INDEX_SYSTEM = "spark.databricks.labs.mosaic.index.system"
    val MOSAIC_GEOMETRY_API = "spark.databricks.labs.mosaic.geometry.api"
    val MOSAIC_RASTER_API = "spark.databricks.labs.mosaic.raster.api"
    val MOSAIC_GDAL_PREFIX = "spark.databricks.labs.mosaic.gdal."
    val MOSAIC_GDAL_NATIVE = "spark.databricks.labs.mosaic.gdal.native"
    val MOSAIC_RASTER_CHECKPOINT = "spark.databricks.labs.mosaic.raster.checkpoint"
    val MOSAIC_RASTER_CHECKPOINT_DEFAULT = "/dbfs/tmp/mosaic/raster/checkpoint"
    val MOSAIC_RASTER_USE_CHECKPOINT = "spark.databricks.labs.mosaic.raster.use.checkpoint"
    val MOSAIC_RASTER_USE_CHECKPOINT_DEFAULT = "false"
    val MOSAIC_RASTER_TMP_PREFIX = "spark.databricks.labs.mosaic.raster.tmp.prefix"
    val MOSAIC_RASTER_TMP_PREFIX_DEFAULT = "/tmp"
    val MOSAIC_RASTER_BLOCKSIZE = "spark.databricks.labs.mosaic.raster.blocksize"
    val MOSAIC_RASTER_BLOCKSIZE_DEFAULT = "128"

    val MOSAIC_RASTER_READ_STRATEGY = "raster.read.strategy"
    val MOSAIC_RASTER_READ_IN_MEMORY = "in_memory"
    val MOSAIC_RASTER_READ_AS_PATH = "as_path"
    val MOSAIC_RASTER_RE_TILE_ON_READ = "retile_on_read"

    val MOSAIC_NO_DRIVER = "no_driver"
    val MOSAIC_TEST_MODE = "spark.databricks.labs.mosaic.test.mode"


    def read: MosaicDataFrameReader = new MosaicDataFrameReader(SparkSession.builder().getOrCreate())

}
