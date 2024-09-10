package com.databricks.labs.mosaic.functions

import com.databricks.labs.mosaic._
import com.databricks.labs.mosaic.core.index.IndexSystemFactory
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{RuntimeConfig, SparkSession}

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
case class ExprConfig(configs: Map[String, String]) {

    /** fluent. */
    def updateSparkConf(): ExprConfig = {
        // populate initial set configs
        val spark = SparkSession.builder().getOrCreate()
        updateSparkConf(spark) // <- returns `this`
    }

    /** fluent. */
    def updateSparkConf(spark: SparkSession): ExprConfig = {
        ExprConfig.updateConfig(this, spark) // <- returns `this`
    }

    def getTestMode: String = {
        configs.getOrElse(MOSAIC_TEST_MODE, "false")
    }

    def setTestMode(testMode: String): ExprConfig = {
        ExprConfig(configs + (MOSAIC_TEST_MODE -> testMode))
    }

    def isTestMode: Boolean = {
        Try(getTestMode == "true").getOrElse(false)
    }

    def isUriDeepCheck: Boolean = {
        Try(getUriDeepCheck).getOrElse(false)
    }

    def getManualCleanupMode: String = {
        configs.getOrElse(MOSAIC_MANUAL_CLEANUP_MODE, "false")
    }

    def setManualCleanupMode(mode: String): ExprConfig = {
        ExprConfig(configs + (MOSAIC_MANUAL_CLEANUP_MODE -> mode))
    }

    def isManualCleanupMode: Boolean = {
        Try(getManualCleanupMode == "true").getOrElse(false)
    }

    def getGDALConf: Map[String, String] = {
        configs.filter { case (k, _) => k.startsWith(MOSAIC_GDAL_PREFIX) }
    }

    def getGeometryAPI: String = configs.getOrElse(MOSAIC_GEOMETRY_API, JTS.name)

    def getRasterCheckpoint: String = configs.getOrElse(MOSAIC_RASTER_CHECKPOINT, MOSAIC_RASTER_CHECKPOINT_DEFAULT)

    def getRasterUseCheckpoint: String = configs.getOrElse(MOSAIC_RASTER_USE_CHECKPOINT, MOSAIC_RASTER_USE_CHECKPOINT_DEFAULT)

    def isRasterUseCheckpoint: Boolean = {
        Try(getRasterUseCheckpoint == "true").getOrElse(false)
    }

    def getCellIdType: DataType = IndexSystemFactory.getIndexSystem(getIndexSystem).cellIdType

    def getIndexSystem: String = configs.getOrElse(MOSAIC_INDEX_SYSTEM, H3.name)

    def getRasterBlockSize: Int = configs.getOrElse(MOSAIC_RASTER_BLOCKSIZE, MOSAIC_RASTER_BLOCKSIZE_DEFAULT).toInt

    def getTmpPrefix: String = configs.getOrElse(MOSAIC_RASTER_TMP_PREFIX, MOSAIC_RASTER_TMP_PREFIX_DEFAULT)

    def getCleanUpAgeLimitMinutes: Int = configs.getOrElse(MOSAIC_CLEANUP_AGE_LIMIT_MINUTES, MOSAIC_CLEANUP_AGE_LIMIT_DEFAULT).toInt

    def getUriDeepCheck: Boolean = configs.getOrElse(MOSAIC_URI_DEEP_CHECK, MOSAIC_URI_DEEP_CHECK_DEFAULT).toBoolean


    def setGDALConf(conf: RuntimeConfig): ExprConfig = {
        val toAdd = conf.getAll.filter(_._1.startsWith(MOSAIC_GDAL_PREFIX))
        ExprConfig(configs ++ toAdd)
    }

    def setGeometryAPI(api: String): ExprConfig = {
        ExprConfig(configs + (MOSAIC_GEOMETRY_API -> api))
    }

    def setIndexSystem(system: String): ExprConfig = {
        ExprConfig(configs + (MOSAIC_INDEX_SYSTEM -> system))
    }

    def setRasterAPI(api: String): ExprConfig = {
        ExprConfig(configs + (MOSAIC_RASTER_API -> api))
    }

    def setRasterCheckpoint(checkpoint: String): ExprConfig = {
        ExprConfig(configs + (MOSAIC_RASTER_CHECKPOINT -> checkpoint))
    }

    def setRasterUseCheckpoint(checkpoint: String): ExprConfig = {
        ExprConfig(configs + (MOSAIC_RASTER_USE_CHECKPOINT -> checkpoint))
    }

    def setTmpPrefix(prefix: String): ExprConfig = {
        ExprConfig(configs + (MOSAIC_RASTER_TMP_PREFIX -> prefix))
    }

    def setCleanUpAgeLimitMinutes(limit: String): ExprConfig = {
        ExprConfig(configs + (MOSAIC_CLEANUP_AGE_LIMIT_MINUTES -> limit))
    }

    def setCleanUpAgeLimitMinutes(limit: Int): ExprConfig = {
        setCleanUpAgeLimitMinutes(limit.toString)
    }

    def setUriDeepCheck(deep: String): ExprConfig = {
        ExprConfig(configs + (MOSAIC_URI_DEEP_CHECK -> deep))
    }

    def setUriDeepCheck(deep: Boolean): ExprConfig = {
        setUriDeepCheck(deep.toString)
    }

    def setConfig(key: String, value: String): ExprConfig = {
        ExprConfig(configs + (key -> value))
    }

}

/**
  * Companion object for the Mosaic Expression Config. Provides constructors
  * from spark session configuration.
  */
object ExprConfig {

    def apply(spark: SparkSession): ExprConfig = {
        val exprConfig = new ExprConfig(Map.empty[String, String])
        this.updateConfig(exprConfig, spark)
    }

    def updateConfig(exprConfig: ExprConfig, spark: SparkSession): ExprConfig = {

        // - make sure spark is in sync with any already set expr configs
        val sparkConf = spark.sparkContext.getConf
        exprConfig.configs.foreach { case (k, v) => sparkConf.set(k, v) }

        // - update any missing expr configs
        // - update the expr values with defaults if missing
        // - this does not set spark configs for expr defaults (when missing)
        exprConfig
            .setGeometryAPI(spark.conf.get(MOSAIC_GEOMETRY_API, JTS.name))
            .setIndexSystem(spark.conf.get(MOSAIC_INDEX_SYSTEM, H3.name))
            .setRasterCheckpoint(spark.conf.get(MOSAIC_RASTER_CHECKPOINT, MOSAIC_RASTER_CHECKPOINT_DEFAULT))
            .setRasterUseCheckpoint(spark.conf.get(MOSAIC_RASTER_USE_CHECKPOINT, MOSAIC_RASTER_USE_CHECKPOINT_DEFAULT))
            .setTmpPrefix(spark.conf.get(MOSAIC_RASTER_TMP_PREFIX, MOSAIC_RASTER_TMP_PREFIX_DEFAULT))
            .setGDALConf(spark.conf)
            .setTestMode(spark.conf.get(MOSAIC_TEST_MODE, "false"))
            .setManualCleanupMode(spark.conf.get(MOSAIC_MANUAL_CLEANUP_MODE, "false"))
            .setCleanUpAgeLimitMinutes(spark.conf.get(MOSAIC_CLEANUP_AGE_LIMIT_MINUTES, MOSAIC_CLEANUP_AGE_LIMIT_DEFAULT))
            .setUriDeepCheck(spark.conf.get(MOSAIC_URI_DEEP_CHECK, MOSAIC_URI_DEEP_CHECK_DEFAULT))
    }
}
