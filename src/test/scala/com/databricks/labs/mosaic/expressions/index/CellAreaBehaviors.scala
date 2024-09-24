package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{MosaicSpatialQueryTest, mocks}
import org.apache.spark.sql.{Row}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers._

trait CellAreaBehaviors extends MosaicSpatialQueryTest {

    def behaviorComputedColumns(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)
        val sc = spark
        import sc.implicits._

        val (cellId, area) = mc.getIndexSystem match {
            case H3IndexSystem        => ("871969500ffffff", 4.327624974422719);
            case BNGIndexSystem       => ("TQ388791", 0.01);
            case CustomIndexSystem(_) => return;
        }

        val result = Seq(cellId).toDF("cellId").select(grid_cellarea($"cellId")).collect()
        math.abs(result.head.getDouble(0) - Row(area).getDouble(0)) < 1e-6 shouldEqual true

        Seq(cellId).toDF("cellId").createOrReplaceTempView("cellId")

        val sqlResult = spark
            .sql("""with subquery (
                   | select grid_cellarea(cellId) as area from cellId
                   |) select * from subquery""".stripMargin)
            .as[Double]
            .collect()

        math.abs(sqlResult.head - area) < 1e-6 shouldEqual true
    }

    def columnFunctionSignatures(mosaicContext: MosaicContext): Unit = {
        val funcs = mosaicContext.functions
        noException should be thrownBy funcs.grid_cellarea(col("wkt"))
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val sc = spark
        import sc.implicits._
        val mc = mosaicContext
        mc.register(spark)

        val wkt = mocks.getWKTRowsDf(mc.getIndexSystem).limit(1).select("wkt").as[String].collect().head
        val cellAreaExpr = CellArea(
          lit(wkt).expr,
          mc.getIndexSystem,
          mc.getGeometryAPI.name
        )

        cellAreaExpr.dataType shouldEqual DoubleType

        val badExpr = CellArea(
          lit(true).expr,
          mc.getIndexSystem,
          mc.getGeometryAPI.name
        )

        an[Error] should be thrownBy badExpr.inputTypes

        noException should be thrownBy mc.functions.grid_cellarea(lit(""))
        noException should be thrownBy mc.functions.grid_cellarea(lit(""))

        noException should be thrownBy cellAreaExpr.makeCopy(cellAreaExpr.children.toArray)
        noException should be thrownBy cellAreaExpr.withNewChildrenInternal(Array(cellAreaExpr.children: _*))
    }

}
