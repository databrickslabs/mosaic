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
trait GeometryKLoopBehaviors extends MosaicSpatialQueryTest {

    def behavior(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)

        val k = 3
        val resolution = 4

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
              grid_geometrykloop(col("wkt"), resolution, k)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length
    }

    def columnFunctionSignatures(mosaicContext: MosaicContext): Unit = {
        val funcs = mosaicContext.functions
        noException should be thrownBy funcs.grid_geometrykloop(col("wkt"), lit(3), lit(3))
        noException should be thrownBy funcs.grid_geometrykloop(col("wkt"), lit(3), 3)
        noException should be thrownBy funcs.grid_geometrykloop(col("wkt"), 3, lit(3))
        noException should be thrownBy funcs.grid_geometrykloop(col("wkt"), 3, 3)
        noException should be thrownBy funcs.grid_geometrykloop(col("wkt"), "3", lit(3))
        noException should be thrownBy funcs.grid_geometrykloop(col("wkt"), "3", 3)
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val sc = spark
        import sc.implicits._
        val mc = mosaicContext
        mc.register(spark)

        val wkt = mocks.getWKTRowsDf(mc.getIndexSystem).limit(1).select("wkt").as[String].collect().head
        val k = 4
        val resolution = 3

        val geometryKLoopExpr = GeometryKLoop(
          lit(wkt).expr,
          lit(resolution).expr,
          lit(k).expr,
          mc.getIndexSystem,
          mc.getGeometryAPI.name
        )

        mc.getIndexSystem match {
            case H3IndexSystem  => geometryKLoopExpr.dataType shouldEqual ArrayType(LongType)
            case BNGIndexSystem => geometryKLoopExpr.dataType shouldEqual ArrayType(StringType)
            case _  => geometryKLoopExpr.dataType shouldEqual ArrayType(LongType)
        }

        val badExpr = GeometryKLoop(
          lit(10).expr,
          lit(resolution).expr,
          lit(true).expr,
          mc.getIndexSystem,
          mc.getGeometryAPI.name
        )

        an[Error] should be thrownBy badExpr.inputTypes

        noException should be thrownBy mc.functions.grid_geometrykloop(lit(""), lit(resolution), lit(k))
        noException should be thrownBy mc.functions.grid_geometrykloop(lit(""), lit(resolution), k)
        noException should be thrownBy mc.functions.grid_geometrykloop(lit(""), resolution, lit(k))
        noException should be thrownBy mc.functions.grid_geometrykloop(lit(""), resolution, k)

        noException should be thrownBy geometryKLoopExpr.makeCopy(geometryKLoopExpr.children.toArray)
    }

}
