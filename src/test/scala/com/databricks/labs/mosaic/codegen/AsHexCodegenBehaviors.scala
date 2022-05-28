package com.databricks.labs.mosaic.codegen

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.getHexRowsDf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.{be, noException}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions.col

trait AsHexCodegenBehaviors { this: AnyFlatSpec =>

    def codeGeneration(mosaicContext: => MosaicContext): Unit = {
        val mc = mosaicContext
        import mc.functions._

        val hexDf: DataFrame = getHexRowsDf(mc).withColumn("asHex", as_hex(col("hex")))
        val queryExecution = hexDf.queryExecution
        val plan = queryExecution.executedPlan

        plan.find(_.isInstanceOf[WholeStageCodegenExec]).isDefined shouldBe true

        val codeGenStage = plan.find(_.isInstanceOf[WholeStageCodegenExec]).get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        hexDf.count() should be > 0L
    }
}
