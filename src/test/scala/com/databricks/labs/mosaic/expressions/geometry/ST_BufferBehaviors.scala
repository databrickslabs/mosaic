package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import com.databricks.labs.mosaic.test.mocks.getWKTRowsDf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers.{an, be, convertToAnyShouldWrapper}

trait ST_BufferBehaviors extends QueryTest {

    def bufferBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        val sc = spark
        import sc.implicits._
        mc.register(spark)

        val referenceGeoms = mocks
            .getWKTRowsDf()
            .orderBy("id")
            .select("wkt")
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        val expected = referenceGeoms.map(_.buffer(1).getLength)
        val result = mocks
            .getWKTRowsDf()
            .orderBy("id")
            .select(st_length(st_buffer($"wkt", lit(1))))
            .as[Double]
            .collect()

        result.zip(expected).foreach { case (l, r) => math.abs(l - r) should be < 1e-8 }

        mocks.getWKTRowsDf().createOrReplaceTempView("source")

        val sqlResult = spark
            .sql("select id, st_length(st_buffer(wkt, 1.0)) from source")
            .orderBy("id")
            .drop("id")
            .as[Double]
            .collect()

        sqlResult.zip(expected).foreach { case (l, r) => math.abs(l - r) should be < 1e-8 }
    }

    def bufferCodegen(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val result = mocks
            .getWKTRowsDf()
            .select(st_length(st_buffer($"wkt", lit(1))))

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val stBuffer = ST_Buffer(lit(1).expr, lit(1).expr, mc.expressionConfig)
        val ctx = new CodegenContext
        an[Error] should be thrownBy stBuffer.genCode(ctx)
    }

    def auxiliaryMethods(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(spark)
        import mc.functions._


        val df = getWKTRowsDf()

        val stBuffer = ST_Buffer(df.col("wkt").expr, lit(1).expr, mc.expressionConfig)

        stBuffer.left shouldEqual df.col("wkt").expr
        stBuffer.right shouldEqual lit(1).expr
        stBuffer.dataType shouldEqual df.col("wkt").expr.dataType
        noException should be thrownBy stBuffer.makeCopy(Array(stBuffer.left, stBuffer.right))

        st_buffer(col("wkt"), 1).expr.children(1) shouldEqual lit(1.0).cast("double").expr

    }

}
