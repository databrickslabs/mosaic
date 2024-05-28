package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers.{an, be, convertToAnyShouldWrapper, noException}

trait ST_TranslateBehaviors extends QueryTest {

    def translateBehaviour(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val multiPoint = List("MULTIPOINT (-70 35, -80 45, -70 45, -80 35)")

        val results = multiPoint
            .toDF("multiPoint")
            .crossJoin(multiPoint.toDF("other"))
            .withColumn("result", st_convexhull($"multiPoint"))
            .select($"result")
            .select(st_translate($"result", lit(1.1), lit(1.1)))
            .as[String]

        results.collect().length > 0 shouldBe true

    }

    def translateCodegen(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val results = mocks
            .getWKTRowsDf()
            .select(st_translate($"wkt", lit(1.1), lit(1.1)))

        val queryExecution = results.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val stTranslate = ST_Translate(lit(1).expr, lit(1.1).expr, lit(1.2).expr, mc.expressionConfig)
        val ctx = new CodegenContext
        an[Error] should be thrownBy stTranslate.genCode(ctx)
    }

    def auxiliaryMethods(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(spark)

        val stTranslate = ST_Translate(lit("POINT (1 1)").expr, lit(1.1).expr, lit(1.2).expr, mc.expressionConfig)

        stTranslate.first shouldEqual lit("POINT (1 1)").expr
        stTranslate.second shouldEqual lit(1.1).expr
        stTranslate.third shouldEqual lit(1.2).expr
        stTranslate.dataType shouldEqual lit("POINT (1 1)").expr.dataType
        noException should be thrownBy stTranslate.makeCopy(stTranslate.children.toArray)
    }

}
