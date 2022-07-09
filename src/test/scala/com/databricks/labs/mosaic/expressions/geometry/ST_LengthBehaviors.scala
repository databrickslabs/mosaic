package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers.{an, be, convertToAnyShouldWrapper, noException}

trait ST_LengthBehaviors extends QueryTest {

    def lengthBehaviour(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val sc = spark
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val expected = mocks
            .getWKTRowsDf(mc)
            .orderBy("id")
            .select("wkt")
            .as[String]
            .collect()
            .map(wkt => mc.getGeometryAPI.geometry(wkt, "WKT").getLength)

        val result = mocks
            .getWKTRowsDf(mc)
            .orderBy("id")
            .select(st_length($"wkt"))
            .as[Double]
            .collect()

        result.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        val result2 = mocks
            .getWKTRowsDf(mc)
            .select(st_perimeter($"wkt"))
            .as[Double]
            .collect()

        result2.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        mocks.getWKTRowsDf(mc).createOrReplaceTempView("source")

        val sqlResult = spark
            .sql("select st_length(wkt) from source")
            .as[Double]
            .collect()

        sqlResult.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        val sqlResult2 = spark
            .sql("select st_perimeter(wkt) from source")
            .as[Double]
            .collect()

        sqlResult2.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def lengthCodegen(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val result = mocks
            .getWKTRowsDf(mc)
            .select(st_length($"wkt"))
            .as[Double]

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val stLength = ST_Length(lit("POINT (1 1)").expr, "illegalAPI")
        val ctx = new CodegenContext
        an[IllegalArgumentException] should be thrownBy stLength.genCode(ctx)
    }

    def auxiliaryMethods(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(spark)

        val stLength = ST_Length(lit("POLYGON (1 1, 2 2, 3 3, 4 4, 1 1)").expr, geometryAPI.name)

        stLength.child shouldEqual lit("POLYGON (1 1, 2 2, 3 3, 4 4, 1 1)").expr
        stLength.dataType shouldEqual DoubleType
        noException should be thrownBy stLength.makeCopy(stLength.children.toArray)

    }

}
