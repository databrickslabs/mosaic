package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import com.databricks.labs.mosaic.test.mocks.getWKTRowsDf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DoubleType
import org.scalatest.matchers.must.Matchers.{be, noException}
import org.scalatest.matchers.should.Matchers.{an, convertToAnyShouldWrapper}

trait ST_AreaBehaviors extends QueryTest {

    def areaBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        val sc = spark
        import sc.implicits._
        mc.register(spark)

        val df = mocks.getWKTRowsDf(mc)

        val expected = df
            .orderBy("id")
            .select("wkt")
            .as[String]
            .collect()
            .map(wkt => geometryAPI.geometry(wkt, "WKT").getArea)
            .map(Row(_))

        val results = df.select(st_area($"wkt"))

        checkAnswer(results, expected)

        mocks.getWKTRowsDf(mc).createOrReplaceTempView("source")

        val sqlResult = spark.sql("select st_area(wkt) from source")

        checkAnswer(sqlResult, expected)
    }

    def areaCodegen(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        val sc = spark
        import sc.implicits._
        mc.register(spark)

        val df = mocks.getWKTRowsDf(mc)

        val result = df.select(st_area($"wkt"))

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val ctx = new CodegenContext
        an [IllegalArgumentException] should be thrownBy ST_Area(lit(1).expr, "JTS").genCode(ctx)
        an [Error] should be thrownBy ST_Area(lit("POINT (1 1)").expr, "Illegal").genCode(ctx)
    }

    def auxiliaryMethods(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(spark)

        val df = getWKTRowsDf(mc)

        val stArea = ST_Area(df.col("wkt").expr, geometryAPI.name)

        stArea.child shouldEqual df.col("wkt").expr
        stArea.dataType shouldEqual DoubleType
        noException should be thrownBy stArea.makeCopy(Array(stArea.child))
        noException should be thrownBy ST_Area.unapply(stArea)
        noException should be thrownBy ST_Area.apply(stArea.child, geometryAPI.name)
    }

}
