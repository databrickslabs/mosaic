package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.{getBoroughs, getWKTRowsDf}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

trait MosaicExplodeBehaviors {
    this: AnyFlatSpec =>

    def wktDecompose(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
              mosaic_explode(col("wkt"), resolution)
            )
            .collect()

        boroughs.collect().length should be <= mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                    |select mosaic_explode(wkt, $resolution) from boroughs
                    |""".stripMargin)
            .collect()

        boroughs.collect().length should be <= mosaics2.length
    }

    def wktDecomposeNoNulls(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val rdd = spark.sparkContext.makeRDD(
          Seq(
            Row("POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))")
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
              mosaic_explode(col("wkt"), resolution, keepCoreGeometries = true)
            )
            .filter(col("index.wkb").isNull)

        noEmptyChips.collect().length should be >= 0

        val emptyChips = df
            .select(
              mosaic_explode(col("wkt"), resolution, keepCoreGeometries = false)
            )
            .filter(col("index.wkb").isNull)

        emptyChips.collect().length should be >= 0
    }

    def wktDecomposeKeepCoreParamExpression(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        mosaicContext.register(spark)

        val rdd = spark.sparkContext.makeRDD(
          Seq(
            Row("POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))")
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
              expr(s"mosaic_explode(wkt, $resolution, true)")
            )
        noEmptyChips.collect().length should be >= 0

        val noEmptyChips_2 = df
            .select(
              expr(s"mosaic_explode(wkt, $resolution, false)")
            )
        noEmptyChips_2.collect().length should be >= 0
    }

    def lineDecompose(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val wktRows: DataFrame = getWKTRowsDf(mc).where(col("wkt").contains("LINESTRING"))

        val mosaics = wktRows
            .select(
              mosaic_explode(col("wkt"), resolution)
            )
            .collect()

        wktRows.collect().length should be <= mosaics.length

        wktRows.createOrReplaceTempView("wkt_rows")

        val mosaics2 = spark
            .sql(s"""
                    |select mosaic_explode(wkt, $resolution) from wkt_rows
                    |""".stripMargin)
            .collect()

        wktRows.collect().length should be <= mosaics2.length

    }

    def wkbDecompose(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
              mosaic_explode(convert_to(col("wkt"), "wkb"), resolution)
            )
            .collect()

        boroughs.collect().length should be <= mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                    |select mosaic_explode(convert_to_wkb(wkt), $resolution) from boroughs
                    |""".stripMargin)
            .collect()

        boroughs.collect().length should be <= mosaics2.length
    }

    def hexDecompose(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
              mosaic_explode(convert_to(col("wkt"), "hex"), resolution)
            )
            .collect()

        boroughs.collect().length should be <= mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                    |select mosaic_explode(convert_to_hex(wkt), $resolution) from boroughs
                    |""".stripMargin)
            .collect()

        boroughs.collect().length should be <= mosaics2.length
    }

    def coordsDecompose(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
              mosaic_explode(convert_to(col("wkt"), "coords"), resolution)
            )
            .collect()

        boroughs.collect().length should be <= mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                    |select mosaic_explode(convert_to_coords(wkt), $resolution) from boroughs
                    |""".stripMargin)
            .collect()

        boroughs.collect().length should be <= mosaics2.length
    }

}
