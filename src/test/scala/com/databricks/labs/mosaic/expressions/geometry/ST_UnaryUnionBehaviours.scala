package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{MosaicSpatialQueryTest, mocks}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers.{an, be, convertToAnyShouldWrapper}

trait ST_UnaryUnionBehaviours extends MosaicSpatialQueryTest {

    def behavior(mc: MosaicContext): Unit = {
        val sc = spark
        mc.register(sc)
        import sc.implicits._
        import mc.functions._

        val input = List("MULTIPOLYGON (((10 10, 20 10, 20 20, 10 20, 10 10)), ((15 15, 25 15, 25 25, 15 25, 15 15)))").toDF("input_geom")
        val expected = List("POLYGON ((20 15, 20 10, 10 10, 10 20, 15 20, 15 25, 25 25, 25 15, 20 15))").toDF("result_geom")
        val result = input.withColumn("result_geom", st_unaryunion(col("input_geom")))

        checkGeometryTopo(mc, result, expected, "result_geom")
    }

    def codegenCompilation(mc: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")

        val sc = spark
        mc.register(sc)
        import sc.implicits._
        import mc.functions._

        val result = mocks.getWKTRowsDf().select(st_unaryunion($"wkt"))

        val plan = result.queryExecution.executedPlan
        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])
        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()
        noException should be thrownBy CodeGenerator.compile(code)

        val stUnaryUnion = ST_UnaryUnion(lit(1).expr, mc.expressionConfig)
        val ctx = new CodegenContext
        an[Error] should be thrownBy stUnaryUnion.genCode(ctx)
    }

    def auxiliaryMethods(mc: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")

        val sc = spark
        mc.register(sc)

        val input = "MULTIPOLYGON (((10 10, 20 10, 20 20, 10 20, 10 10)), ((15 15, 25 15, 25 25, 15 25, 15 15)))"

        val stUnaryUnion = ST_UnaryUnion(lit(input).expr, mc.expressionConfig)
        stUnaryUnion.child shouldEqual lit(input).expr
        stUnaryUnion.dataType shouldEqual lit(input).expr.dataType
        noException should be thrownBy stUnaryUnion.makeCopy(Array(stUnaryUnion.child))
    }

}
