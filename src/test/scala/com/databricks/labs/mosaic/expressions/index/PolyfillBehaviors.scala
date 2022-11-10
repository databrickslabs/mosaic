package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import com.databricks.labs.mosaic.test.mocks.getBoroughs
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers._

//noinspection ScalaDeprecation
trait PolyfillBehaviors extends QueryTest {

    def polyfillOnComputedColumns(indexSystem: IndexSystem, geometryAPI: GeometryAPI, resolution: Int): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
              grid_polyfill(convert_to(col("wkt"), "wkb"), resolution)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length
    }

    def wktPolyfill(indexSystem: IndexSystem, geometryAPI: GeometryAPI, resolution: Int): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

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

    def wkbPolyfill(indexSystem: IndexSystem, geometryAPI: GeometryAPI, resolution: Int): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

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

    def hexPolyfill(indexSystem: IndexSystem, geometryAPI: GeometryAPI, resolution: Int): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

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

    def coordsPolyfill(indexSystem: IndexSystem, geometryAPI: GeometryAPI, resolution: Int): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

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

    def auxiliaryMethods(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val sc = spark
        import sc.implicits._
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(spark)

        val wkt = mocks.getWKTRowsDf(mc).limit(1).select("wkt").as[String].collect().head
        val resExpr = mc.getIndexSystem match {
            case H3IndexSystem  => lit(mc.getIndexSystem.resolutions.head).expr
            case BNGIndexSystem => lit("100m").expr
        }

        val polyfillExpr = Polyfill(
          lit(wkt).expr,
          resExpr,
          mc.getIndexSystem.name,
          mc.getGeometryAPI.name
        )

        mc.getIndexSystem match {
            case H3IndexSystem  => polyfillExpr.dataType shouldEqual ArrayType(LongType)
            case BNGIndexSystem => polyfillExpr.dataType shouldEqual ArrayType(StringType)
        }

        val badExpr = Polyfill(
          lit(10).expr,
          lit(true).expr,
          mc.getIndexSystem.name,
          mc.getGeometryAPI.name
        )

        an[Error] should be thrownBy badExpr.inputTypes

        // legacy API def tests
        noException should be thrownBy mc.functions.polyfill(lit(""), lit(5))
        noException should be thrownBy mc.functions.polyfill(lit(""), 5)
    }

}
