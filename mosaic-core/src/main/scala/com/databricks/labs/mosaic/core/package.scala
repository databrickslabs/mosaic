package com.databricks.labs.mosaic

/**
 * This package object contains all the constants used in the Mosaic library.
 */
package object core {

  val DATABRICKS_SQL_FUNCTIONS_MODULE = "com.databricks.sql.functions"
  val SPARK_DATABRICKS_GEO_H3_ENABLED = "spark.databricks.geo.h3.enabled"

  val MOSAIC_INDEX_SYSTEM = "spark.databricks.labs.mosaic.index.system"
  val MOSAIC_INDEX_SYSTEM_FACTORY: String = "spark.databricks.labs.mosaic.index.system.factory"
  val MOSAIC_GEOMETRY_API = "spark.databricks.labs.mosaic.geometry.api"
  val MOSAIC_RASTER_API = "spark.databricks.labs.mosaic.raster.api"
  val MOSAIC_GDAL_NATIVE = "spark.databricks.labs.mosaic.gdal.native"
  val MOSAIC_RASTER_CHECKPOINT = "spark.databricks.labs.mosaic.raster.checkpoint"
  val MOSAIC_RASTER_CHECKPOINT_DEFAULT = "dbfs:/tmp/mosaic/raster/checkpoint"

}
