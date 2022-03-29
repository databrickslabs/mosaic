package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.{getBoroughs, getWKTRowsDf}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

trait MosaicExplodeBehaviors {
    this: AnyFlatSpec =>

    def wktDecompose(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs

        val mosaics = boroughs
            .select(
              mosaic_explode(col("wkt"), 11)
            )
            .collect()

        boroughs.collect().length should be < mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql("""
                   |select mosaic_explode(wkt, 11) from boroughs
                   |""".stripMargin)
            .collect()

        boroughs.collect().length should be < mosaics2.length
    }

    def lineDecompose(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val wktRows: DataFrame = getWKTRowsDf

        val mosaics = wktRows
            .select(
                mosaic_explode(col("wkt"), 4)
            )
            .collect()

        wktRows.collect().length should be < mosaics.length

        wktRows.createOrReplaceTempView("wkt_rows")

        val mosaics2 = spark
            .sql("""
                   |select mosaic_explode(wkt, 4) from wkt_rows
                   |""".stripMargin)
            .collect()

        wktRows.collect().length should be < mosaics2.length
    }

    def wkbDecompose(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs

        val mosaics = boroughs
            .select(
              mosaic_explode(convert_to(col("wkt"), "wkb"), 11)
            )
            .collect()

        boroughs.collect().length should be < mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql("""
                   |select mosaic_explode(convert_to_wkb(wkt), 11) from boroughs
                   |""".stripMargin)
            .collect()

        boroughs.collect().length should be < mosaics2.length
    }

    def hexDecompose(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs

        val mosaics = boroughs
            .select(
              mosaic_explode(convert_to(col("wkt"), "hex"), 11)
            )
            .collect()

        boroughs.collect().length should be < mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql("""
                   |select mosaic_explode(convert_to_hex(wkt), 11) from boroughs
                   |""".stripMargin)
            .collect()

        boroughs.collect().length should be < mosaics2.length
    }

    def coordsDecompose(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs

        val mosaics = boroughs
            .select(
              mosaic_explode(convert_to(col("wkt"), "coords"), 11)
            )
            .collect()

        boroughs.collect().length should be < mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql("""
                   |select mosaic_explode(convert_to_coords(wkt), 11) from boroughs
                   |""".stripMargin)
            .collect()

        boroughs.collect().length should be < mosaics2.length
    }

}
