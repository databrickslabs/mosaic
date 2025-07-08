package com.databricks.labs.mosaic.functions

import com.databricks.labs.mosaic._
import com.databricks.labs.mosaic.core.index.IndexSystemFactory
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{RuntimeConfig, SparkSession}
import org.apache.spark.util.SerializableConfiguration

import scala.util.Try

/**
  * Mosaic Expression Config is a class that contains the configuration for the
  * Mosaic Expression. Singleton objects are not accessible outside the JVM, so
  * this is the mechanism to allow for shared context. This is used to control
  * for the Mosaic runtime APIs and checkpoint locations.
  *
  * @param configs
  *   The configuration map for the Mosaic Expression.
  */
case class MosaicExpressionConfig(
    configs: Map[String, String],
    hConf: SerializableConfiguration
) extends Serializable {

    def updateSparkConf(): Unit = {
        // populate initial set configs
        val spark = SparkSession.builder().getOrCreate()
        updateSparkConf(spark)
    }

    def updateSparkConf(spark: SparkSession): Unit = {
        val sparkConf = spark.sparkContext.getConf
        configs.foreach { case (k, v) => sparkConf.set(k, v) }

        val hConf = new SerializableConfiguration(spark.sessionState.newHadoopConf())

        // update defaults as well
        this
            .setGeometryAPI(spark.conf.get(MOSAIC_GEOMETRY_API, JTS.name))
            .setIndexSystem(spark.conf.get(MOSAIC_INDEX_SYSTEM, H3.name))
            .setRasterCheckpoint(spark.conf.get(MOSAIC_RASTER_CHECKPOINT, MOSAIC_RASTER_CHECKPOINT_DEFAULT))
            .setRasterUseCheckpoint(spark.conf.get(MOSAIC_RASTER_USE_CHECKPOINT, MOSAIC_RASTER_USE_CHECKPOINT_DEFAULT))
            .setTmpPrefix(spark.conf.get(MOSAIC_RASTER_TMP_PREFIX, "/tmp"))
            .setGDALConf(spark.conf)
    }

    def getGDALConf: Map[String, String] = {
        configs.filter { case (k, _) => k.startsWith(MOSAIC_GDAL_PREFIX) }
    }

    def getGeometryAPI: String = configs.getOrElse(MOSAIC_GEOMETRY_API, JTS.name)

    def getRasterCheckpoint: String = configs.getOrElse(MOSAIC_RASTER_CHECKPOINT, MOSAIC_RASTER_CHECKPOINT_DEFAULT)

    def getRasterUseCheckpoint: String = configs.getOrElse(MOSAIC_RASTER_USE_CHECKPOINT, MOSAIC_RASTER_USE_CHECKPOINT_DEFAULT)

    def getTmpPrefix: String = configs.getOrElse(MOSAIC_RASTER_TMP_PREFIX, MOSAIC_RASTER_TMP_PREFIX_DEFAULT)

    def isRasterUseCheckpoint: Boolean = {
        Try(getRasterUseCheckpoint == "true").getOrElse(false)
    }

    def getCellIdType: DataType = IndexSystemFactory.getIndexSystem(getIndexSystem).cellIdType

    def getIndexSystem: String = configs.getOrElse(MOSAIC_INDEX_SYSTEM, H3.name)

    def getRasterBlockSize: Int = configs.getOrElse(MOSAIC_RASTER_BLOCKSIZE, MOSAIC_RASTER_BLOCKSIZE_DEFAULT).toInt

    def setGDALConf(conf: RuntimeConfig): MosaicExpressionConfig = {
        val toAdd = conf.getAll.filter(_._1.startsWith(MOSAIC_GDAL_PREFIX))
        MosaicExpressionConfig(configs ++ toAdd, hConf)
    }

    def setGeometryAPI(api: String): MosaicExpressionConfig = {
        MosaicExpressionConfig(configs + (MOSAIC_GEOMETRY_API -> api), hConf)
    }

    def setIndexSystem(system: String): MosaicExpressionConfig = {
        MosaicExpressionConfig(configs + (MOSAIC_INDEX_SYSTEM -> system), hConf)
    }

    def setRasterAPI(api: String): MosaicExpressionConfig = {
        MosaicExpressionConfig(configs + (MOSAIC_RASTER_API -> api), hConf)
    }

    def setRasterCheckpoint(checkpoint: String): MosaicExpressionConfig = {
        MosaicExpressionConfig(configs + (MOSAIC_RASTER_CHECKPOINT -> checkpoint), hConf)
    }

    def setRasterUseCheckpoint(checkpoint: String): MosaicExpressionConfig = {
        MosaicExpressionConfig(configs + (MOSAIC_RASTER_USE_CHECKPOINT -> checkpoint), hConf)
    }

    def setTmpPrefix(prefix: String): MosaicExpressionConfig = {
        MosaicExpressionConfig(configs + (MOSAIC_RASTER_TMP_PREFIX -> prefix), hConf)
    }

    def setConfig(key: String, value: String): MosaicExpressionConfig = {
        MosaicExpressionConfig(configs + (key -> value), hConf)
    }

    def getTileMaxSize: Long = configs.getOrElse(MOSAIC_TILE_SIZE, MOSAIC_TILE_SIZE_DEFAULT).toLong

    def setHadoopConf(hConf: SerializableConfiguration): MosaicExpressionConfig = {
        MosaicExpressionConfig(configs, hConf)
    }

}

/**
  * Companion object for the Mosaic Expression Config. Provides constructors
  * from spark session configuration.
  */
object MosaicExpressionConfig {

    def apply(spark: SparkSession): MosaicExpressionConfig = {
        val hConf = new SerializableConfiguration(spark.sessionState.newHadoopConf())
        val expressionConfig = new MosaicExpressionConfig(Map.empty[String, String], hConf)
        expressionConfig
            .setGeometryAPI(spark.conf.get(MOSAIC_GEOMETRY_API, JTS.name))
            .setIndexSystem(spark.conf.get(MOSAIC_INDEX_SYSTEM, H3.name))
            .setRasterCheckpoint(spark.conf.get(MOSAIC_RASTER_CHECKPOINT, MOSAIC_RASTER_CHECKPOINT_DEFAULT))
            .setRasterUseCheckpoint(spark.conf.get(MOSAIC_RASTER_USE_CHECKPOINT, MOSAIC_RASTER_USE_CHECKPOINT_DEFAULT))
            .setTmpPrefix(spark.conf.get(MOSAIC_RASTER_TMP_PREFIX, MOSAIC_RASTER_TMP_PREFIX_DEFAULT))
            .setGDALConf(spark.conf)
    }

}
