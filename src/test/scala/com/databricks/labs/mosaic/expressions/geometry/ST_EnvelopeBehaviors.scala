package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{MosaicSpatialQueryTest, mocks}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers.{an, be, convertToAnyShouldWrapper}

trait ST_EnvelopeBehaviors extends MosaicSpatialQueryTest {

    def behavior(mc: MosaicContext): Unit = {
        val sc = spark
        mc.register(sc)
        import sc.implicits._
        import mc.functions._

        val input = List("POLYGON ((10 10, 20 10, 15 20, 10 10))").toDF("input_geom")
        val expected = List("POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))").toDF("result_geom")
        val result = input.withColumn("result_geom", st_envelope(col("input_geom")))

        checkGeometryTopo(mc, result, expected, "result_geom")
    }

    def codegenCompilation(mc: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")

        val sc = spark
        mc.register(sc)
        import sc.implicits._
        import mc.functions._

        val result = mocks.getWKTRowsDf().select(st_envelope($"wkt"))

        val plan = result.queryExecution.executedPlan
        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])
        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()
        noException should be thrownBy CodeGenerator.compile(code)

        val stEnvelope = ST_Envelope(lit(1).expr, mc.expressionConfig)
        val ctx = new CodegenContext
        an[Error] should be thrownBy stEnvelope.genCode(ctx)
    }

    def auxiliaryMethods(mc: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")

        val sc = spark
        mc.register(sc)

        val input = "POLYGON (10 10, 20 10, 15 20, 10 10)"

        val stEnvelope = ST_Envelope(lit(input).expr, mc.expressionConfig)
        stEnvelope.child shouldEqual lit(input).expr
        stEnvelope.dataType shouldEqual lit(input).expr.dataType
        noException should be thrownBy stEnvelope.makeCopy(Array(stEnvelope.child))
    }

}
