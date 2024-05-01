package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions.lit
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers.{an, be, convertToAnyShouldWrapper}

trait ST_ConcaveHullBehaviors extends QueryTest {

    def concaveHullBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        val sc = spark
        import sc.implicits._
        mc.register(spark)

        val multiPoint = List("MULTIPOINT (-70 35, -72 40, -78 40, -80 45, -70 45, -80 35)")
        val expected = List("POLYGON ((-78 40, -80 45, -72 40, -70 45, -70 35, -80 35, -78 40))")
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        val results = multiPoint
            .toDF("multiPoint")
            .crossJoin(multiPoint.toDF("other"))
            .withColumn("result", st_concavehull($"multiPoint", 0.1))
            .select($"result")
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        results.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        noException should be thrownBy multiPoint.toDF("multiPoint")
            .withColumn("result", st_concavehull($"multiPoint", 0.01, allowHoles = true))
            .select($"result")
            .as[String]
            .collect()

        multiPoint.toDF("multiPoint").createOrReplaceTempView("multiPoint")

        spark.sql("SELECT ST_ConcaveHull(multiPoint, 0.01, true) FROM multiPoint")
            .as[String]
            .collect()

    }

    def concaveHullCodegen(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val multiPoint = List("MULTIPOINT (-70 35, -80 45, -70 45, -80 35)").toDF("multiPoint")

        val result = multiPoint
            .withColumn("result", st_concavehull($"multiPoint", 0.01))
            .select(st_asbinary($"result"))

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val stConvexHull = ST_ConvexHull(lit(1).expr, mc.expressionConfig)
        val ctx = new CodegenContext
        an[Error] should be thrownBy stConvexHull.genCode(ctx)
    }

    def auxiliaryMethods(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(spark)

        val stConcaveHull = ST_ConcaveHull(lit("MULTIPOINT (-70 35, -80 45, -70 45, -80 35)").expr, lit(0.01).expr, lit(true).expr, mc.expressionConfig)

        stConcaveHull.children.length shouldEqual 3
        stConcaveHull.first shouldEqual lit("MULTIPOINT (-70 35, -80 45, -70 45, -80 35)").expr
        stConcaveHull.second shouldEqual lit(0.01).expr
        stConcaveHull.third shouldEqual lit(true).expr

        stConcaveHull.makeCopy(Array(lit("MULTIPOINT (-70 35, -80 45, -70 45, -80 35)").expr, lit(0.01).expr, lit(true).expr)) shouldEqual stConcaveHull

    }

}
