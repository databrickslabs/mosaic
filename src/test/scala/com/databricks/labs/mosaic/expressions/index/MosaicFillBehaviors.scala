package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.getBoroughs
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

trait MosaicFillBehaviors {
    this: AnyFlatSpec =>

    def wktMosaicFill(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
              mosaicfill(col("wkt"), resolution)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                   |select mosaicfill(wkt, $resolution) from boroughs
                   |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    def wkbMosaicFill(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
              mosaicfill(convert_to(col("wkt"), "wkb"), resolution)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                   |select mosaicfill(convert_to_wkb(wkt), $resolution) from boroughs
                   |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    def hexMosaicFill(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
              mosaicfill(convert_to(col("wkt"), "hex"), resolution)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                   |select mosaicfill(convert_to_hex(wkt), $resolution) from boroughs
                   |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    def coordsMosaicFill(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
              mosaicfill(convert_to(col("wkt"), "coords"), resolution)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                   |select mosaicfill(convert_to_coords(wkt), $resolution) from boroughs
                   |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    def wktMosaicFillKeepCoreGeom(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
          .select(
              mosaicfill(col("wkt"), resolution, keepCoreGeometries = true)
          )
          .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
          .sql(s"""
                 |select mosaicfill(wkt, $resolution, true) from boroughs
                 |""".stripMargin)
          .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

}
