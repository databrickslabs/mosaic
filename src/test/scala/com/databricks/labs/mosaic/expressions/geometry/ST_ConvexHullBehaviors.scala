package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions.lit
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers.{an, be, convertToAnyShouldWrapper}

trait ST_ConvexHullBehaviors extends QueryTest {

    def convexHullBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        val sc = spark
        import sc.implicits._
        mc.register(spark)

        val multiPoint = List("MULTIPOINT (-70 35, -80 45, -70 45, -80 35)")
        val expected = List("POLYGON ((-80 35, -80 45, -70 45, -70 35, -80 35))")
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        val results = multiPoint
            .toDF("multiPoint")
            .crossJoin(multiPoint.toDF("other"))
            .withColumn("result", st_convexhull($"multiPoint"))
            .select($"result")
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        results.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def convexHullCodegen(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val multiPoint = List("MULTIPOINT (-70 35, -80 45, -70 45, -80 35)").toDF("multiPoint")
        val expected = List("POLYGON ((-80 35, -80 45, -70 45, -70 35, -80 35))").toDF("polygon")

        val result = multiPoint
            .crossJoin(expected)
            .withColumn("result", st_convexhull($"multiPoint"))
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

        val stConvexHull = ST_ConvexHull(lit("MULTIPOINT (-70 35, -80 45, -70 45, -80 35)").expr, mc.expressionConfig)

        stConvexHull.child shouldEqual lit("MULTIPOINT (-70 35, -80 45, -70 45, -80 35)").expr
        stConvexHull.dataType shouldEqual lit("MULTIPOINT (-70 35, -80 45, -70 45, -80 35)").expr.dataType
        noException should be thrownBy stConvexHull.makeCopy(Array(stConvexHull.child))

    }

}
