package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import com.databricks.labs.mosaic.test.mocks.getWKTRowsDf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers.{an, be, convertToAnyShouldWrapper}

trait ST_SimplifyBehaviors extends QueryTest {

    def simplifyBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val referenceGeoms = mocks
            .getWKTRowsDf()
            .orderBy("id")
            .select("wkt")
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKT"))
        val expected = referenceGeoms.map(_.simplify(1).getLength)

        // test st_simplify(Column, Double)
        val result1 = mocks
            .getWKTRowsDf()
            .orderBy("id")
            .select(st_length(st_simplify($"wkt", 1.0)))
            .as[Double]
            .collect()
        result1.zip(expected).foreach { case (l, r) => math.abs(l - r) should be < 1e-8 }

        // test st_simplify(Column, Column)
        val result2 = mocks
            .getWKTRowsDf()
            .orderBy("id")
            .select(st_length(st_simplify($"wkt", lit(1.0))))
            .as[Double]
            .collect()
        result2.zip(expected).foreach { case (l, r) => math.abs(l - r) should be < 1e-8 }
    }

    def simplifyCodegen(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val result = mocks
            .getWKTRowsDf()
            .select(st_length(st_simplify($"wkt", 1.0)))

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val stSimplify = ST_Simplify(lit(1).expr, lit(1).expr, mc.expressionConfig)
        val ctx = new CodegenContext
        an[Error] should be thrownBy stSimplify.genCode(ctx)
    }

    def auxiliaryMethods(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(spark)
        import mc.functions._

        val df = getWKTRowsDf()

        val stSimplify = ST_Simplify(df.col("wkt").expr, lit(1).expr, mc.expressionConfig)

        stSimplify.left shouldEqual df.col("wkt").expr
        stSimplify.right shouldEqual lit(1).expr
        stSimplify.dataType shouldEqual df.col("wkt").expr.dataType
        noException should be thrownBy stSimplify.makeCopy(Array(stSimplify.left, stSimplify.right))

        st_simplify(col("wkt"), 1).expr.children(1) shouldEqual lit(1.0).cast("double").expr

    }

}
