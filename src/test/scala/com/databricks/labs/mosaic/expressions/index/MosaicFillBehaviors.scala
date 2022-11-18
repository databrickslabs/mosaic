package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import com.databricks.labs.mosaic.test.mocks.getBoroughs
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{size => arrayColumnSize}
import org.apache.spark.sql.types._
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

    def wktMosaicTessellate(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        val sc = spark
        mc.register(sc)
        import sc.implicits._
        import mc.functions._

        val geom = Seq("POLYGON ((5.26 52.72, 5.20 52.71, 5.21 52.75, 5.26 52.75, 5.26 52.72))").toDF("wkt")
        val mosaics = geom
            .select(
              grid_tessellate(col("wkt"), resolution).alias("tessellation")
            )
            .select(arrayColumnSize($"tessellation.chips").alias("number_of_chips"))
            .select($"number_of_chips")
            .collect()
            .map(_.getInt(0))

        mosaics.foreach { case i => assert(i > 0, "tessellation has no chips") }
    }

    def auxiliaryMethods(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
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

        val mosaicfillExpr = MosaicFill(
          lit(wkt).expr,
          resExpr,
          lit(false).expr,
          idAsLongExpr,
          mc.getIndexSystem.name,
          mc.getGeometryAPI.name
        )

        mosaicfillExpr.first shouldEqual lit(wkt).expr
        mosaicfillExpr.second shouldEqual resExpr
        mosaicfillExpr.third shouldEqual lit(false).expr
        mosaicfillExpr.fourth shouldEqual idAsLongExpr

        mc.getIndexSystem match {
            case H3IndexSystem  => mosaicfillExpr.inputTypes should contain theSameElementsAs
                    Seq(StringType, IntegerType, BooleanType, BooleanType)
            case BNGIndexSystem => mosaicfillExpr.inputTypes should contain theSameElementsAs
                    Seq(StringType, StringType, BooleanType, BooleanType)
        }

        val badExpr = MosaicFill(
          lit(10).expr,
          resExpr,
          lit(false).expr,
          idAsLongExpr,
          mc.getIndexSystem.name,
          mc.getGeometryAPI.name
        )

        an[Error] should be thrownBy badExpr.inputTypes
        an[Error] should be thrownBy badExpr
            .makeCopy(Array(lit(wkt).expr, resExpr, lit(5).expr, lit(5).expr))
            .dataType

        // legacy API def tests
        noException should be thrownBy mc.functions.mosaicfill(lit(""), lit(5))
        noException should be thrownBy mc.functions.mosaicfill(lit(""), 5)
        noException should be thrownBy mc.functions.mosaicfill(lit(""), lit(5), lit(true))
        noException should be thrownBy mc.functions.mosaicfill(lit(""), lit(5), true)
        noException should be thrownBy mc.functions.mosaicfill(lit(""), 5, true)
    }

}
