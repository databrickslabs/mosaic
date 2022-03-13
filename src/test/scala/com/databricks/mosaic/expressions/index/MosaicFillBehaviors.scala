package com.databricks.mosaic.expressions.index

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.test.mocks.getBoroughs

trait MosaicFillBehaviors {
    this: AnyFlatSpec =>

    def wktMosaicFill(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs

        val mosaics = boroughs
            .select(
              mosaicfill(col("wkt"), 11)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql("""
                   |select mosaicfill(wkt, 11) from boroughs
                   |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    def wkbMosaicFill(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs

        val mosaics = boroughs
            .select(
              mosaicfill(convert_to(col("wkt"), "wkb"), 11)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql("""
                   |select mosaicfill(convert_to_wkb(wkt), 11) from boroughs
                   |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    def hexMosaicFill(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs

        val mosaics = boroughs
            .select(
              mosaicfill(convert_to(col("wkt"), "hex"), 11)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql("""
                   |select mosaicfill(convert_to_hex(wkt), 11) from boroughs
                   |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    def coordsMosaicFill(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs

        val mosaics = boroughs
            .select(
              mosaicfill(convert_to(col("wkt"), "coords"), 11)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql("""
                   |select mosaicfill(convert_to_coords(wkt), 11) from boroughs
                   |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

}
