package com.databricks.labs.mosaic.expressions.raster

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{element_at, lit, map_keys}

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

trait RasterBehaviors { this: AnyFlatSpec =>

    def readST_MetaData(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        mc.enableGDAL(spark)

        val rasterDfWithMetadata = mocks
            .getGeotiffBinaryDf(spark)
            .select(st_metadata($"content").alias("metadata"))

        val result = rasterDfWithMetadata.as[Map[String, String]].collect()

        result.head.getOrElse("SHORTNAME", "") shouldBe "MCD43A4"
        result.head.getOrElse("ASSOCIATEDINSTRUMENTSHORTNAME", "") shouldBe "MODIS"
        result.head.getOrElse("RANGEBEGINNINGDATE", "") shouldBe "2018-06-26"
        result.head.getOrElse("RANGEENDINGDATE", "") shouldBe "2018-07-11"
        result.head.getOrElse("TileID", "") shouldBe "51010007"
    }
    def readST_Subdatasets(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        mc.enableGDAL(spark)

        val rasterDfWithSubdatasets = mocks
            .getNetCDFBinaryDf(spark)
            .select(
              st_subdatasets($"content")
                  .alias("subdatasets")
            )

        val result = rasterDfWithSubdatasets.as[Map[String, String]].collect()

        result.head.keys.toList.length shouldBe 2
        result.head.values.toList should contain allElementsOf List(
          "[1x3600x7200] //bleaching_alert_area (8-bit unsigned character)",
          "[1x3600x7200] //mask (8-bit unsigned character)"
        )
    }

    def readST_BandMetaData(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        mc.enableGDAL(spark)

        val rasterDfWithBandMetadata = mocks
            .getNetCDFBinaryDf(spark)
            .withColumn("subdatasets", st_subdatasets($"content"))
            .withColumn("bleachingSubdataset", element_at(map_keys($"subdatasets"), 1))
            .select(
              st_bandmetadata($"content", lit(1), $"bleachingSubdataset")
                  .alias("metadata")
            )

        rasterDfWithBandMetadata.show(false)

        val result = rasterDfWithBandMetadata.as[Map[String, String]].collect()

        result.head.getOrElse("bleaching_alert_area_long_name", "") shouldBe "bleaching alert area 7-day maximum composite"
        result.head.getOrElse("bleaching_alert_area_valid_max", "") shouldBe "4 "
        result.head.getOrElse("bleaching_alert_area_valid_min", "") shouldBe "0 "
        result.head.getOrElse("bleaching_alert_area_units", "") shouldBe "stress_level"
    }

}
