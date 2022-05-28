package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.{getBoroughs, getWKTRowsDf}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

trait MosaicExplodeBehaviors {
    this: AnyFlatSpec =>

    def wktDecompose(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

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

    def wktDecomposeNoNulls(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val rdd = spark.sparkContext.makeRDD(
          Seq(
            Row("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))")
          )
        )
        val schema = StructType(
          List(
            StructField("wkt", StringType)
          )
        )
        val df = spark.createDataFrame(rdd, schema)

        val noEmptyChips = df
            .select(
              mosaic_explode(col("wkt"), 4, keepCoreGeometries = true)
            )
            .filter(col("index.wkb").isNull)
            .count()

        noEmptyChips should equal(0)

        val emptyChips = df
            .select(
              mosaic_explode(col("wkt"), 4, keepCoreGeometries = false)
            )
            .filter(col("index.wkb").isNull)

        emptyChips.collect().length should be > 0
    }

    def wktDecomposeKeepCoreParamExpression(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        mosaicContext.register(spark)

        val rdd = spark.sparkContext.makeRDD(
          Seq(
            Row("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))")
          )
        )
        val schema = StructType(
          List(
            StructField("wkt", StringType)
          )
        )
        val df = spark.createDataFrame(rdd, schema)

        val noEmptyChips = df
            .select(
              expr("mosaic_explode(wkt, 4, true)")
            )
        noEmptyChips.collect().length should be > 0

        val noEmptyChips_2 = df
            .select(
              expr("mosaic_explode(wkt, 4, false)")
            )
        noEmptyChips_2.collect().length should be > 0
    }

    def lineDecompose(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val wktRows: DataFrame = getWKTRowsDf(mc)

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

        val boroughs: DataFrame = getBoroughs(mc)

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

        val boroughs: DataFrame = getBoroughs(mc)

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

        val boroughs: DataFrame = getBoroughs(mc)

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
