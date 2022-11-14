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
trait CellKDiscExplodeBehaviors extends MosaicSpatialQueryTest {

    def behaviorComputedColumns(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)

        val k = 3
        val resolution = 4

        val boroughs: DataFrame = getBoroughs(mc)
            .withColumn("centroid", st_centroid2D(col("wkt")))
            .withColumn("point", st_point(col("centroid.x"), col("centroid.y")))
            .withColumn("cell_id", grid_pointascellid(col("point"), resolution))

        val mosaics = boroughs
            .select(
              col("id"),
              grid_cellkdiscexplode(col("cell_id"), k).alias("kdisc")
            )
            .groupBy(col("id"))
            .agg(collect_set("kdisc"))
            .collect()

        boroughs.collect().length shouldEqual mosaics.length
    }

    def columnFunctionSignatures(mosaicContext: MosaicContext): Unit = {
        val funcs = mosaicContext.functions
        noException should be thrownBy funcs.grid_cellkdiscexplode(col("wkt"), lit(3))
        noException should be thrownBy funcs.grid_cellkdiscexplode(col("wkt"), 3)
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = mosaicContext
        mc.register(spark)
        val sc = spark
        import sc.implicits._

        val wkt = mocks.getWKTRowsDf(mc).limit(1).select("wkt").as[String].collect().head
        val k = 4

        val cellKDiscExplodeExpr = CellKDiscExplode(
          lit(wkt).expr,
          lit(k).expr,
          mc.getIndexSystem.name,
          mc.getGeometryAPI.name
        )
        val withNull = cellKDiscExplodeExpr.copy(cellId = lit(null).expr)

        cellKDiscExplodeExpr.position shouldEqual false
        cellKDiscExplodeExpr.inline shouldEqual false
        cellKDiscExplodeExpr.checkInputDataTypes() shouldEqual TypeCheckResult.TypeCheckSuccess
        withNull.eval(InternalRow.fromSeq(Seq(null, null))) shouldEqual Seq.empty

        val badExpr = CellKDiscExplode(
          lit(10).expr,
          lit(k).expr,
          mc.getIndexSystem.name,
          mc.getGeometryAPI.name
        )

        badExpr.checkInputDataTypes().isFailure shouldEqual true
        badExpr
            .withNewChildren(Array(lit(wkt).expr, lit(true).expr))
            .checkInputDataTypes()
            .isFailure shouldEqual true

        // Default getters
        noException should be thrownBy cellKDiscExplodeExpr.cellId
        noException should be thrownBy cellKDiscExplodeExpr.k

        noException should be thrownBy mc.functions.grid_cellkdiscexplode(lit(""), lit(5))
        noException should be thrownBy mc.functions.grid_cellkdiscexplode(lit(""), 5)
    }

}
