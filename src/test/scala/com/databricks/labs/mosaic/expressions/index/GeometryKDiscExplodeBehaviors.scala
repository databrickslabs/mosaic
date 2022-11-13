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
trait GeometryKDiscExplodeBehaviors extends MosaicSpatialQueryTest {

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
              grid_geometrykdiscexplode(col("wkt"), resolution, k).alias("kdisc")
            )
            .groupBy(col("id"))
            .agg(collect_set("kdisc"))
            .collect()

        boroughs.collect().length shouldEqual mosaics.length
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = mosaicContext
        mc.register(spark)
        val sc = spark
        import sc.implicits._

        val wkt = mocks.getWKTRowsDf(mc).limit(1).select("wkt").as[String].collect().head
        val k = 4
        val resolution = 3

        val geomKDiscExplodeExpr = GeometryKDiscExplode(
          lit(wkt).expr,
          lit(resolution).expr,
          lit(k).expr,
          mc.getIndexSystem.name,
          mc.getGeometryAPI.name
        )

        val withNull = geomKDiscExplodeExpr.copy(geom = lit(null).expr)

        geomKDiscExplodeExpr.position shouldEqual false
        geomKDiscExplodeExpr.inline shouldEqual false
        geomKDiscExplodeExpr.checkInputDataTypes() shouldEqual TypeCheckResult.TypeCheckSuccess
        withNull.eval(InternalRow.fromSeq(Seq(null, null, null))) shouldEqual Seq.empty

        val badExpr = GeometryKDiscExplode(
          lit(10).expr,
          lit(resolution).expr,
          lit(k).expr,
          mc.getIndexSystem.name,
          mc.getGeometryAPI.name
        )

        badExpr.checkInputDataTypes().isFailure shouldEqual true
        badExpr
            .withNewChildren(Array(lit(wkt).expr, lit(true).expr, lit(true).expr))
            .checkInputDataTypes()
            .isFailure shouldEqual true
        geomKDiscExplodeExpr
            .copy(k = lit(true).expr)
            .checkInputDataTypes()
            .isFailure shouldEqual true

        // Default getters
        noException should be thrownBy geomKDiscExplodeExpr.geom
        noException should be thrownBy geomKDiscExplodeExpr.resolution
        noException should be thrownBy geomKDiscExplodeExpr.k

        noException should be thrownBy mc.functions.grid_geometrykdiscexplode(lit(""), lit(5), lit(5))
        noException should be thrownBy mc.functions.grid_geometrykdiscexplode(lit(""), lit(5), 5)
        noException should be thrownBy mc.functions.grid_geometrykdiscexplode(lit(""), 5, lit(5))
        noException should be thrownBy mc.functions.grid_geometrykdiscexplode(lit(""), 5, 5)
    }

}
