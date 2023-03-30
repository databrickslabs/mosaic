package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{mocks, MosaicSpatialQueryTest}
import com.databricks.labs.mosaic.test.mocks.getBoroughs
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers._

//noinspection ScalaDeprecation
trait GeometryKRingExplodeBehaviors extends MosaicSpatialQueryTest {

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
              col("id"),
              grid_geometrykringexplode(col("wkt"), resolution, k).alias("kring")
            )
            .groupBy(col("id"))
            .agg(collect_set("kring"))
            .collect()

        boroughs.collect().length shouldEqual mosaics.length
    }

    def columnFunctionSignatures(mosaicContext: MosaicContext): Unit = {
        val funcs = mosaicContext.functions
        noException should be thrownBy funcs.grid_geometrykringexplode(col("wkt"), lit(3), lit(3))
        noException should be thrownBy funcs.grid_geometrykringexplode(col("wkt"), lit(3), 3)
        noException should be thrownBy funcs.grid_geometrykringexplode(col("wkt"), 3, lit(3))
        noException should be thrownBy funcs.grid_geometrykringexplode(col("wkt"), 3, 3)
        noException should be thrownBy funcs.grid_geometrykringexplode(col("wkt"), "3", lit(3))
        noException should be thrownBy funcs.grid_geometrykringexplode(col("wkt"), "3", 3)
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = mosaicContext
        mc.register(spark)
        val sc = spark
        import sc.implicits._

        val wkt = mocks.getWKTRowsDf(mc.getIndexSystem).limit(1).select("wkt").as[String].collect().head
        val k = 4
        val resolution = 3

        val geomKRingExplodeExpr = GeometryKRingExplode(
          lit(wkt).expr,
          lit(resolution).expr,
          lit(k).expr,
          mc.getIndexSystem,
          mc.getGeometryAPI.name
        )
        val withNull = geomKRingExplodeExpr.copy(geom = lit(null).expr)

        geomKRingExplodeExpr.position shouldEqual false
        geomKRingExplodeExpr.inline shouldEqual false
        geomKRingExplodeExpr.checkInputDataTypes() shouldEqual TypeCheckResult.TypeCheckSuccess
        withNull.eval(InternalRow.fromSeq(Seq(null, null, null))) shouldEqual Seq.empty

        val badExpr = GeometryKRingExplode(
          lit(10).expr,
          lit(10).expr,
          lit(k).expr,
          mc.getIndexSystem,
          mc.getGeometryAPI.name
        )

        badExpr.checkInputDataTypes().isFailure shouldEqual true
        badExpr
            .withNewChildren(Array(lit(wkt).expr, lit(true).expr, lit(true).expr))
            .checkInputDataTypes()
            .isFailure shouldEqual true
        geomKRingExplodeExpr
            .copy(k = lit(true).expr)
            .checkInputDataTypes()
            .isFailure shouldEqual true


        // Default getters
        noException should be thrownBy geomKRingExplodeExpr.geom
        noException should be thrownBy geomKRingExplodeExpr.resolution
        noException should be thrownBy geomKRingExplodeExpr.k

        noException should be thrownBy mc.functions.grid_geometrykringexplode(lit(""), lit(5), lit(2))
        noException should be thrownBy mc.functions.grid_geometrykringexplode(lit(""), lit(5), 2)
        noException should be thrownBy mc.functions.grid_geometrykringexplode(lit(""), 5, lit(2))
        noException should be thrownBy mc.functions.grid_geometrykringexplode(lit(""), 5, 2)
    }

}
