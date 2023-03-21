package com.databricks.labs

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.api.RasterAPI
import com.databricks.labs.mosaic.datasource.multiread.MosaicDataFrameReader
import org.apache.spark.sql.SparkSession

package object mosaic {

    val JTS: GeometryAPI = mosaic.core.geometry.api.JTS
    val ESRI: GeometryAPI = mosaic.core.geometry.api.ESRI
    val GDAL: RasterAPI = mosaic.core.raster.api.RasterAPI.GDAL
    val H3: IndexSystem = mosaic.core.index.H3IndexSystem
    val BNG: IndexSystem = mosaic.core.index.BNGIndexSystem

    val DATABRICKS_SQL_FUNCTIONS_MODULE = "com.databricks.sql.functions"
    val SPARK_DATABRICKS_GEO_H3_ENABLED = "spark.databricks.geo.h3.enabled"

    val MOSAIC_INDEX_SYSTEM = "spark.databricks.labs.mosaic.index.system"
    val MOSAIC_GEOMETRY_API = "spark.databricks.labs.mosaic.geometry.api"
    val MOSAIC_RASTER_API = "spark.databricks.labs.mosaic.raster.api"
    val MOSAIC_GDAL_NATIVE = "spark.databricks.labs.mosaic.gdal.native"
    val MOSAIC_RASTER_CHECKPOINT = "spark.databricks.labs.mosaic.raster.checkpoint"
    val MOSAIC_RASTER_CHECKPOINT_DEFAULT = "dbfs:/tmp/mosaic/raster/checkpoint"

    def read: MosaicDataFrameReader = new MosaicDataFrameReader(SparkSession.builder().getOrCreate())

}
