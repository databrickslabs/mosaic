package com.databricks.labs.mosaic.core.expressions

import com.databricks.labs.mosaic.core.GenericServiceFactory.{GeometryAPIFactory, IndexSystemFactory, RasterAPIFactory}
import com.databricks.labs.mosaic.core._
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.RasterAPI
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataType

/**
 * Mosaic Expression Config is a class that contains the configuration for the
 * Mosaic Expression. Singleton objects are not accessible outside the JVM, so
 * this is the mechanism to allow for shared context. This is used to control
 * for the Mosaic runtime APIs and checkpoint locations.
 *
 * @param configs
 * The configuration map for the Mosaic Expression.
 */
case class MosaicExpressionConfig(configs: Map[String, String]) {

  def updateSparkConf(): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val sparkConf = spark.sparkContext.getConf
    configs.foreach { case (k, v) => sparkConf.set(k, v) }
  }

  def getGeometryAPI(params: Array[Object] = Array.empty): GeometryAPI = GeometryAPIFactory.getGeometryAPI(configs(MOSAIC_GEOMETRY_API), params)

  def getIndexSystem(params: Array[Object] = Array.empty): IndexSystem = IndexSystemFactory.getIndexSystem(configs(MOSAIC_INDEX_SYSTEM), params)

  def getRasterAPI(params: Array[Object] = Array.empty): RasterAPI = RasterAPIFactory.getRasterAPI(configs(MOSAIC_RASTER_API), params)

  def getRasterCheckpoint: String = configs.getOrElse(MOSAIC_RASTER_CHECKPOINT, MOSAIC_RASTER_CHECKPOINT_DEFAULT)

  def getCellIdType: DataType = getIndexSystem().cellIdType

  def setGeometryAPI(api: String): MosaicExpressionConfig = {
    MosaicExpressionConfig(configs + (MOSAIC_GEOMETRY_API -> api))
  }

  def setIndexSystem(system: String): MosaicExpressionConfig = {
    MosaicExpressionConfig(configs + (MOSAIC_INDEX_SYSTEM -> system))
  }

  def setRasterAPI(api: String): MosaicExpressionConfig = {
    MosaicExpressionConfig(configs + (MOSAIC_RASTER_API -> api))
  }

  def setRasterCheckpoint(checkpoint: String): MosaicExpressionConfig = {
    MosaicExpressionConfig(configs + (MOSAIC_RASTER_CHECKPOINT -> checkpoint))
  }

  def setConfig(key: String, value: String): MosaicExpressionConfig = {
    MosaicExpressionConfig(configs + (key -> value))
  }

}

/**
 * Companion object for the Mosaic Expression Config. Provides constructors
 * from spark session configuration.
 */
object MosaicExpressionConfig {

  def apply(spark: SparkSession): MosaicExpressionConfig = {
    val expressionConfig = new MosaicExpressionConfig(Map.empty[String, String])
    expressionConfig
      .setGeometryAPI(spark.conf.get(MOSAIC_GEOMETRY_API))
      .setIndexSystem(spark.conf.get(MOSAIC_INDEX_SYSTEM))
      .setRasterAPI(spark.conf.get(MOSAIC_RASTER_API))
      .setRasterCheckpoint(spark.conf.get(MOSAIC_RASTER_CHECKPOINT, MOSAIC_RASTER_CHECKPOINT_DEFAULT))
  }

}
