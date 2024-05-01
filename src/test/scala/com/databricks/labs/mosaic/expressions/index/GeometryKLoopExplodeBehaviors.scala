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
trait GeometryKLoopExplodeBehaviors extends MosaicSpatialQueryTest {

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
              col("id"),
              grid_geometrykloopexplode(col("wkt"), resolution, k).alias("kloop")
            )
            .groupBy(col("id"))
            .agg(collect_set("kloop"))
            .collect()

        boroughs.collect().length shouldEqual mosaics.length
    }

    def columnFunctionSignatures(mosaicContext: MosaicContext): Unit = {
        val funcs = mosaicContext.functions
        noException should be thrownBy funcs.grid_geometrykloopexplode(col("wkt"), lit(3), lit(3))
        noException should be thrownBy funcs.grid_geometrykloopexplode(col("wkt"), lit(3), 3)
        noException should be thrownBy funcs.grid_geometrykloopexplode(col("wkt"), 3, lit(3))
        noException should be thrownBy funcs.grid_geometrykloopexplode(col("wkt"), 3, 3)
        noException should be thrownBy funcs.grid_geometrykloopexplode(col("wkt"), "3", lit(3))
        noException should be thrownBy funcs.grid_geometrykloopexplode(col("wkt"), "3", 3)
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        mc.register(spark)
        val sc = spark
        import sc.implicits._

        val wkt = mocks.getWKTRowsDf(mc.getIndexSystem).limit(1).select("wkt").as[String].collect().head
        val k = 4
        val resolution = 3

        val geomKLoopExplodeExpr = GeometryKLoopExplode(
          lit(wkt).expr,
          lit(resolution).expr,
          lit(k).expr,
          mc.getIndexSystem,
          mc.getGeometryAPI.name
        )

        val withNull = geomKLoopExplodeExpr.copy(geom = lit(null).expr)

        geomKLoopExplodeExpr.position shouldEqual false
        geomKLoopExplodeExpr.inline shouldEqual false
        geomKLoopExplodeExpr.checkInputDataTypes() shouldEqual TypeCheckResult.TypeCheckSuccess
        withNull.eval(InternalRow.fromSeq(Seq(null, null, null))) shouldEqual Seq.empty

        val badExpr = GeometryKLoopExplode(
          lit(10).expr,
          lit(resolution).expr,
          lit(k).expr,
          mc.getIndexSystem,
          mc.getGeometryAPI.name
        )

        badExpr.checkInputDataTypes().isFailure shouldEqual true
        badExpr
            .withNewChildren(Array(lit(wkt).expr, lit(true).expr, lit(true).expr))
            .checkInputDataTypes()
            .isFailure shouldEqual true
        geomKLoopExplodeExpr
            .copy(k = lit(true).expr)
            .checkInputDataTypes()
            .isFailure shouldEqual true

        // Default getters
        noException should be thrownBy geomKLoopExplodeExpr.geom
        noException should be thrownBy geomKLoopExplodeExpr.resolution
        noException should be thrownBy geomKLoopExplodeExpr.k

        noException should be thrownBy mc.functions.grid_geometrykloopexplode(lit(""), lit(5), lit(5))
        noException should be thrownBy mc.functions.grid_geometrykloopexplode(lit(""), lit(5), 5)
        noException should be thrownBy mc.functions.grid_geometrykloopexplode(lit(""), 5, lit(5))
        noException should be thrownBy mc.functions.grid_geometrykloopexplode(lit(""), 5, 5)
    }

}
