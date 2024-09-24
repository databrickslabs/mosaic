package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers.{an, be, convertToAnyShouldWrapper}

trait ST_ZBehaviors extends MosaicSpatialQueryTest {

    def stzBehavior(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        val sc = spark
        import sc.implicits._
        mc.register(spark)

        val rows = List(
          ("POINT (2 3 5)", 5),
          ("POINT (7 11 13)", 13),
          ("POINT (17 19 23)", 23),
          ("POINT (29 31 37)", 37)
        )

        val result = rows
            .toDF("wkt", "expected")
            .withColumn("result", st_z($"wkt"))
            .where($"expected" === $"result")

        result.count shouldBe 4
    }

    def stzCodegen(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val rows = List(
          ("POINT (2 3 5)", 5),
          ("POINT (7 11 13)", 13),
          ("POINT (17 19 23)", 23),
          ("POINT (29 31 37)", 37)
        )

        val points = rows.toDF("wkt", "expected")

        val result = points
            .withColumn("result", st_z($"wkt"))
            .where($"expected" === $"result")

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val stZ = ST_Z(lit(1).expr, mc.expressionConfig)
        val ctx = new CodegenContext
        an[Error] should be thrownBy stZ.genCode(ctx)
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        mc.register(spark)

        val stZ = ST_Z(lit("POINT (2 3 4)").expr, mc.expressionConfig)

        stZ.child shouldEqual lit("POINT (2 3 4)").expr
        stZ.dataType shouldEqual DoubleType
        noException should be thrownBy stZ.makeCopy(Array(stZ.child))
    }

}
