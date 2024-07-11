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
    val MOSAIC_RASTER_API = "spark.databricks.labs.mosaic.tile.api"
    val MOSAIC_GDAL_PREFIX = "spark.databricks.labs.mosaic.gdal."
    val MOSAIC_GDAL_NATIVE = "spark.databricks.labs.mosaic.gdal.native"
    val MOSAIC_RASTER_CHECKPOINT = "spark.databricks.labs.mosaic.tile.checkpoint"
    val MOSAIC_RASTER_CHECKPOINT_DEFAULT = "/dbfs/tmp/mosaic/tile/checkpoint"
    val MOSAIC_RASTER_USE_CHECKPOINT = "spark.databricks.labs.mosaic.tile.use.checkpoint"
    val MOSAIC_RASTER_USE_CHECKPOINT_DEFAULT = "false"
    val MOSAIC_RASTER_TMP_PREFIX = "spark.databricks.labs.mosaic.tile.tmp.prefix"
    val MOSAIC_RASTER_TMP_PREFIX_DEFAULT = "/tmp"
    val MOSAIC_CLEANUP_AGE_LIMIT_MINUTES = "spark.databricks.labs.mosaic.cleanup.age.limit.minutes"
    val MOSAIC_CLEANUP_AGE_LIMIT_DEFAULT = "30"
    val MOSAIC_RASTER_BLOCKSIZE = "spark.databricks.labs.mosaic.tile.blocksize"
    val MOSAIC_RASTER_BLOCKSIZE_DEFAULT = "128"
    val MOSAIC_URI_DEEP_CHECK = "spark.databricks.labs.mosaic.uri.deep.check"
    val MOSAIC_URI_DEEP_CHECK_DEFAULT = "true"

    val BAND_META_SET_KEY = "MOSAIC_BAND_INDEX"
    val BAND_META_GET_KEY = "GDAL_MOSAIC_BAND_INDEX"

    val MOSAIC_RASTER_READ_STRATEGY = "tile.read.strategy"
    val MOSAIC_RASTER_READ_IN_MEMORY = "in_memory"
    val MOSAIC_RASTER_READ_AS_PATH = "as_path"
    val MOSAIC_RASTER_RE_TILE_ON_READ = "retile_on_read"

    val NO_PATH_STRING = "no_path"
    val NO_EXT = "ukn"
    val NO_DRIVER = "no_driver"
    val MOSAIC_TEST_MODE = "spark.databricks.labs.mosaic.test.mode"
    val MOSAIC_MANUAL_CLEANUP_MODE = "spark.databricks.labs.mosaic.manual.cleanup.mode"

    // processing keys
    val RASTER_BAND_INDEX_KEY      = "bandIndex"
    val RASTER_DRIVER_KEY          = "driver"
    val RASTER_PARENT_PATH_KEY     = "parentPath"
    val RASTER_PATH_KEY            = "path"
    val RASTER_SUBDATASET_NAME_KEY = "subdatasetName"

    // informational keys
    val RASTER_ALL_PARENTS_KEY     = "all_parents"
    val RASTER_FULL_ERR_KEY        = "full_error"
    val RASTER_LAST_CMD_KEY        = "last_command"
    val RASTER_LAST_ERR_KEY        = "last_error"
    val RASTER_MEM_SIZE_KEY        = "mem_size"

    val POLYGON_EMPTY_WKT = "POLYGON(EMPTY)"
    val POINT_0_WKT = "POINT(0 0)" // no support for POINT(EMPTY) in WKB

    def read: MosaicDataFrameReader = new MosaicDataFrameReader(SparkSession.builder().getOrCreate())

}
