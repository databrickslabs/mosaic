package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{mocks, MosaicSpatialQueryTest}
import com.databricks.labs.mosaic.test.mocks.getBoroughs
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers._

//noinspection ScalaDeprecation
trait PolyfillBehaviors extends MosaicSpatialQueryTest {

    def polyfillOnComputedColumns(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 11
            case BNGIndexSystem => 4
            case _ => 9
        }

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
              grid_polyfill(convert_to(col("wkt"), "wkb"), resolution)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length
    }

    def wktPolyfill(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 11
            case BNGIndexSystem => 4
            case _ => 9
        }

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
              polyfill(col("wkt"), resolution)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                    |select polyfill(wkt, $resolution) from boroughs
                    |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    def wkbPolyfill(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 11
            case BNGIndexSystem => 4
            case _ => 9
        }

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(convert_to(col("wkt"), "wkb").as("wkb"))
            .select(polyfill(col("wkb"), resolution))
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                    |select polyfill(convert_to_wkb(wkt), $resolution) from boroughs
                    |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    def hexPolyfill(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 11
            case BNGIndexSystem => 4
            case _ => 9
        }

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(convert_to(col("wkt"), "hex").as("hex"))
            .select(polyfill(col("hex"), resolution))
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                    |select polyfill(convert_to_hex(wkt), $resolution) from boroughs
                    |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    def coordsPolyfill(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 11
            case BNGIndexSystem => 4
            case _ => 9
        }

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(convert_to(col("wkt"), "coords").as("coords"))
            .select(polyfill(col("coords"), resolution))
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                    |select polyfill(convert_to_coords(wkt), $resolution) from boroughs
                    |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    def columnFunctionSignatures(mosaicContext: MosaicContext): Unit = {
        val funcs = mosaicContext.functions
        noException should be thrownBy funcs.grid_polyfill(col("wkt"), 3)
        noException should be thrownBy funcs.grid_polyfill(col("wkt"), lit(3))
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val sc = spark
        import sc.implicits._
        val mc = mosaicContext
        mc.register(spark)

        val wkt = mocks.getWKTRowsDf(mc.getIndexSystem).limit(1).select("wkt").as[String].collect().head
        val resExpr = mc.getIndexSystem match {
            case H3IndexSystem  => lit(mc.getIndexSystem.resolutions.head).expr
            case BNGIndexSystem => lit("100m").expr
            case _ => lit("3").expr
        }

        val polyfillExpr = Polyfill(
          lit(wkt).expr,
          resExpr,
          mc.getIndexSystem,
          mc.getGeometryAPI.name
        )

        mc.getIndexSystem match {
            case H3IndexSystem  => polyfillExpr.dataType shouldEqual ArrayType(LongType)
            case BNGIndexSystem => polyfillExpr.dataType shouldEqual ArrayType(StringType)
            case _ => polyfillExpr.dataType shouldEqual ArrayType(LongType)
        }

        val badExpr = Polyfill(
          lit(10).expr,
          lit(true).expr,
          mc.getIndexSystem,
          mc.getGeometryAPI.name
        )

        an[Error] should be thrownBy badExpr.inputTypes

        // legacy API def tests
        noException should be thrownBy mc.functions.polyfill(lit(""), lit(5))
        noException should be thrownBy mc.functions.polyfill(lit(""), 5)

        noException should be thrownBy polyfillExpr.makeCopy(polyfillExpr.children.toArray)
    }

}
