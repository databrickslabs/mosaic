package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{mocks, MosaicSpatialQueryTest}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions.lit
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers.{an, be, convertToAnyShouldWrapper}

trait ST_DifferenceBehaviors extends MosaicSpatialQueryTest {

    def behavior(mc: MosaicContext): Unit = {
        val sc = spark
        mc.register(sc)
        import mc.functions._
        import sc.implicits._

        val left_polygon = List("POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))").toDF("left_geom")
        val right_polygon = List("POLYGON ((15 15, 25 15, 25 25, 15 25, 15 15))").toDF("right_geom")
        val expected = List("POLYGON ((10 10, 20 10, 20 15, 15 15, 15 20, 10 20, 10 10))").toDF("result_geom")

        val result = left_polygon
            .crossJoin(right_polygon)
            .withColumn("result_geom", st_difference($"left_geom", $"right_geom"))

        checkGeometryTopo(mc, result, expected, "result_geom")
    }

    def codegenCompilation(mc: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")

        val sc = spark
        mc.register(sc)
        import mc.functions._
        import sc.implicits._

        val result = mocks.getWKTRowsDf().select(st_difference($"wkt", $"wkt"))

        // Check if code generation was planned
        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan
        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])
        wholeStageCodegenExec.isDefined shouldBe true

        // Check is generated code compiles
        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()
        noException should be thrownBy CodeGenerator.compile(code)

        // Check if invalid code fails code generation
        val stUnion = ST_Difference(lit(1).expr, lit(1).expr, mc.expressionConfig)
        val ctx = new CodegenContext
        an[Error] should be thrownBy stUnion.genCode(ctx)
    }

    def auxiliaryMethods(mc: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")

        val sc = spark
        mc.register(sc)

        val stDifference = ST_Difference(
          lit("POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))").expr,
          lit("POLYGON ((15 15, 25 15, 25 25, 15 25, 15 15))").expr,
          mc.expressionConfig
        )

        stDifference.left shouldEqual lit("POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))").expr
        stDifference.right shouldEqual lit("POLYGON ((15 15, 25 15, 25 25, 15 25, 15 15))").expr
        stDifference.dataType shouldEqual lit("POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))").expr.dataType
        noException should be thrownBy stDifference.makeCopy(Array(stDifference.left, stDifference.right))
    }

}
