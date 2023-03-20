package com.databricks.labs.mosaic.functions

import com.databricks.labs.mosaic._
import com.databricks.labs.mosaic.core.index.IndexSystemFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataType

/**
  * Mosaic Expression Config is a class that contains the configuration for the
  * Mosaic Expression. Singleton objects are not accessible outside the JVM, so
  * this is the mechanism to allow for shared context. This is used to control
  * for the Mosaic runtime APIs and checkpoint locations.
  *
  * @param configs
  *   The configuration map for the Mosaic Expression.
  */
case class MosaicExpressionConfig(configs: Map[String, String]) {

    def updateSparkConf(): Unit = {
        val spark = SparkSession.builder().getOrCreate()
        val sparkConf = spark.sparkContext.getConf
        configs.foreach { case (k, v) => sparkConf.set(k, v) }
    }

    def getGeometryAPI: String = configs.getOrElse(MOSAIC_GEOMETRY_API, JTS.name)

    def getIndexSystem: String = configs.getOrElse(MOSAIC_INDEX_SYSTEM, H3.name)

    def getRasterAPI: String = configs.getOrElse(MOSAIC_RASTER_API, GDAL.name)

    def getRasterCheckpoint: String = configs.getOrElse(MOSAIC_RASTER_CHECKPOINT, MOSAIC_RASTER_CHECKPOINT_DEFAULT)

    def getCellIdType: DataType = IndexSystemFactory.getIndexSystem(getIndexSystem).cellIdType

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
            .setGeometryAPI(spark.conf.get(MOSAIC_GEOMETRY_API, JTS.name))
            .setIndexSystem(spark.conf.get(MOSAIC_INDEX_SYSTEM, H3.name))
            .setRasterAPI(spark.conf.get(MOSAIC_RASTER_API, GDAL.name))
            .setRasterCheckpoint(spark.conf.get(MOSAIC_RASTER_CHECKPOINT, MOSAIC_RASTER_CHECKPOINT_DEFAULT))
    }

}
