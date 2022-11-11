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
trait GeometryKDiscBehaviors extends MosaicSpatialQueryTest {

    def behavior(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)

        val k = 3
        val resolution = 4

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
              grid_geometrykdisc(col("wkt"), resolution, k)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val sc = spark
        import sc.implicits._
        val mc = mosaicContext
        mc.register(spark)

        val wkt = mocks.getWKTRowsDf(mc).limit(1).select("wkt").as[String].collect().head
        val k = 4
        val resolution = 3

        val cellKDiscExpr = GeometryKDisc(
          lit(wkt).expr,
          lit(resolution).expr,
          lit(k).expr,
          mc.getIndexSystem.name,
          mc.getGeometryAPI.name
        )

        mc.getIndexSystem match {
            case H3IndexSystem  => cellKDiscExpr.dataType shouldEqual ArrayType(LongType)
            case BNGIndexSystem => cellKDiscExpr.dataType shouldEqual ArrayType(StringType)
        }

        val badExpr = GeometryKDisc(
          lit(10).expr,
          lit(resolution).expr,
          lit(true).expr,
          mc.getIndexSystem.name,
          mc.getGeometryAPI.name
        )

        an[Error] should be thrownBy badExpr.inputTypes

        noException should be thrownBy mc.functions.grid_geometrykdisc(lit(""), lit(resolution), lit(k))
        noException should be thrownBy mc.functions.grid_geometrykdisc(lit(""), lit(resolution), k)
        noException should be thrownBy mc.functions.grid_geometrykdisc(lit(""), resolution, lit(k))
        noException should be thrownBy mc.functions.grid_geometrykdisc(lit(""), resolution, k)
    }

}
