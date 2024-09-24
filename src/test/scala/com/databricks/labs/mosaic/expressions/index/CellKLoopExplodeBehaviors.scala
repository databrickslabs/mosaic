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
trait CellKLoopExplodeBehaviors extends MosaicSpatialQueryTest {

    def behaviorComputedColumns(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)

        val k = 3
        val resolution = 4

        val boroughs: DataFrame = getBoroughs(mc)
            .withColumn("centroid", st_centroid(col("wkt")))
            .withColumn("cell_id", grid_pointascellid(col("centroid"), resolution))

        val mosaics = boroughs
            .select(
              col("id"),
              grid_cellkloopexplode(col("cell_id"), k).alias("kloop")
            )
            .groupBy(col("id"))
            .agg(collect_set("kloop"))
            .collect()

        boroughs.collect().length shouldEqual mosaics.length
    }

    def columnFunctionSignatures(mosaicContext: MosaicContext): Unit = {
        val funcs = mosaicContext.functions
        noException should be thrownBy funcs.grid_cellkloopexplode(col("wkt"), lit(3))
        noException should be thrownBy funcs.grid_cellkloopexplode(col("wkt"), 3)
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        mc.register(spark)
        val sc = spark
        import sc.implicits._

        val wkt = mocks.getWKTRowsDf(mc.getIndexSystem).limit(1).select("wkt").as[String].collect().head
        val k = 4

        val cellKLoopExplodeExpr = CellKLoopExplode(
          lit(wkt).expr,
          lit(k).expr,
          mc.getIndexSystem,
          mc.getGeometryAPI.name
        )
        val withNull = cellKLoopExplodeExpr.copy(cellId = lit(null).expr)

        cellKLoopExplodeExpr.position shouldEqual false
        cellKLoopExplodeExpr.inline shouldEqual false
        cellKLoopExplodeExpr.checkInputDataTypes() shouldEqual TypeCheckResult.TypeCheckSuccess
        withNull.eval(InternalRow.fromSeq(Seq(null, null))) shouldEqual Seq.empty

        val badExpr = CellKLoopExplode(
          lit(10).expr,
          lit(k).expr,
          mc.getIndexSystem,
          mc.getGeometryAPI.name
        )

        badExpr.checkInputDataTypes().isFailure shouldEqual true
        badExpr
            .withNewChildren(Array(lit(wkt).expr, lit(true).expr))
            .checkInputDataTypes()
            .isFailure shouldEqual true

        // Default getters
        noException should be thrownBy cellKLoopExplodeExpr.cellId
        noException should be thrownBy cellKLoopExplodeExpr.k

        noException should be thrownBy mc.functions.grid_cellkloopexplode(lit(""), lit(5))
        noException should be thrownBy mc.functions.grid_cellkloopexplode(lit(""), 5)
    }

}
