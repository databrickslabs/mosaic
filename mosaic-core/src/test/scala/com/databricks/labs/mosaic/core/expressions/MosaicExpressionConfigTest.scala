package com.databricks.labs.mosaic.core.expressions

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.RasterAPI
import com.databricks.labs.mosaic.core.{MOSAIC_GEOMETRY_API, MOSAIC_INDEX_SYSTEM, MOSAIC_RASTER_API, MOSAIC_RASTER_CHECKPOINT}
import org.apache.spark.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

//noinspection ScalaUnusedSymbol
class MosaicExpressionConfigTest extends AnyFunSuite with SharedSparkContext with MockFactory {

  test("MosaicExpressionConfig") {
    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.setLogLevel("FATAL")

    val mockGeometryAPI = mock[GeometryAPI]
    val mockIndexSystem = mock[IndexSystem]
    val mockRasterAPI = mock[RasterAPI]

    // Class paths match the scalamock macro generated classes, the order needs to be persevered
    val configs = Map(
      MOSAIC_GEOMETRY_API -> "com.databricks.labs.mosaic.core.expressions.MosaicExpressionConfigTest$$anon$1",
      MOSAIC_INDEX_SYSTEM -> "com.databricks.labs.mosaic.core.expressions.MosaicExpressionConfigTest$$anon$2",
      MOSAIC_RASTER_API -> "com.databricks.labs.mosaic.core.expressions.MosaicExpressionConfigTest$$anon$3",
      MOSAIC_RASTER_CHECKPOINT -> "mosaic-raster-checkpoint"
    )

    val mosaicExpressionConfig = MosaicExpressionConfig(configs)

    noException should be thrownBy mosaicExpressionConfig.updateSparkConf()

    noException should be thrownBy mosaicExpressionConfig.getGeometryAPI(Array(this))
    noException should be thrownBy mosaicExpressionConfig.getIndexSystem(Array(this))
    noException should be thrownBy mosaicExpressionConfig.getRasterAPI(Array(this))
    noException should be thrownBy mosaicExpressionConfig.getRasterCheckpoint

    noException should be thrownBy mosaicExpressionConfig.setGeometryAPI("geometryAPI")
    noException should be thrownBy mosaicExpressionConfig.setIndexSystem("indexSystem")
    noException should be thrownBy mosaicExpressionConfig.setRasterAPI("rasterAPI")
    noException should be thrownBy mosaicExpressionConfig.setRasterCheckpoint("rasterCheckpoint")
    noException should be thrownBy mosaicExpressionConfig.setConfig("key", "value")

    spark.conf.set(MOSAIC_GEOMETRY_API, "com.databricks.labs.mosaic.core.expressions.MosaicExpressionConfigTest$$anon$1")
    spark.conf.set(MOSAIC_INDEX_SYSTEM, "com.databricks.labs.mosaic.core.expressions.MosaicExpressionConfigTest$$anon$2")
    spark.conf.set(MOSAIC_RASTER_API, "com.databricks.labs.mosaic.core.expressions.MosaicExpressionConfigTest$$anon$3")
    spark.conf.set(MOSAIC_RASTER_CHECKPOINT, "mosaic-raster-checkpoint")
    MosaicExpressionConfig(spark) shouldBe a[MosaicExpressionConfig]

  }

}
