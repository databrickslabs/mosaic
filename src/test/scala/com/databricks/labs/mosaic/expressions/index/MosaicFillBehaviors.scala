package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import com.databricks.labs.mosaic.test.mocks.getBoroughs
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

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

    def auxiliaryMethods(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        mosaicContext.register(spark)
        val sc = spark
        import sc.implicits._

        val wkt = mocks.getWKTRowsDf(mosaicContext).limit(1).select("wkt").as[String].collect().head
        val idAsLongExpr = mc.getIndexSystem.defaultDataTypeID match {
            case LongType   => lit(true).expr
            case StringType => lit(false).expr
        }
        val resExpr = mc.getIndexSystem match {
            case H3IndexSystem  => lit(mc.getIndexSystem.resolutions.head).expr
            case BNGIndexSystem => lit("100m").expr
        }

        val mosaicExplodeExpr = MosaicExplode(
          lit(wkt).expr,
          resExpr,
          lit(false).expr,
          idAsLongExpr,
          mc.getIndexSystem.name,
          mc.getGeometryAPI.name
        )

        mosaicExplodeExpr.position shouldEqual false
        mosaicExplodeExpr.inline shouldEqual false
        mosaicExplodeExpr.checkInputDataTypes() shouldEqual TypeCheckResult.TypeCheckSuccess

        val badExpr = MosaicExplode(
          lit(10).expr,
          resExpr,
          lit(false).expr,
          idAsLongExpr,
          mc.getIndexSystem.name,
          mc.getGeometryAPI.name
        )

        badExpr.checkInputDataTypes().isFailure shouldEqual true

        //legacy API def tests
        noException should be thrownBy mc.functions.mosaicfill(lit(""), lit(5))
        noException should be thrownBy mc.functions.mosaicfill(lit(""), 5)
        noException should be thrownBy mc.functions.mosaicfill(lit(""), lit(5), lit(true))
        noException should be thrownBy mc.functions.mosaicfill(lit(""), lit(5), true)
        noException should be thrownBy mc.functions.mosaicfill(lit(""), 5, true)
    }

}
