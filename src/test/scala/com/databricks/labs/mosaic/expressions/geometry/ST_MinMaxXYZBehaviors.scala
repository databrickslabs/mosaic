package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.getWKTRowsDf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions.col
import org.scalatest.matchers.must.Matchers.{be, noException}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

trait ST_MinMaxXYZBehaviors extends QueryTest {

    def xMinBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

        val expected = (indexSystem match {
            case H3IndexSystem  => List(10.0, 0.0, 10.0, 10.0, -75.78033, 10.0, 10.0, 10.0)
            case BNGIndexSystem => List(10000.0, 0.0, 10000.0, 10000.0, 75780.0, 10000.0, 10000.0, 10000.0)
        }).map(Row(_))

        val df = getWKTRowsDf(mc).orderBy("id")
        val results = df.select(st_xmin(col("wkt")))

        checkAnswer(results, expected)

        df.createOrReplaceTempView("data")
        val sqlResults = spark.sql("select st_xmin(wkt) from data")

        checkAnswer(sqlResults, expected)
    }

    def xMaxBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

        val expected = (mc.getIndexSystem match {
            case H3IndexSystem  => List(40.0, 2.0, 110.0, 45.0, -75.78033, 40.0, 40.0, 40.0)
            case BNGIndexSystem => List(40000.0, 2000.0, 110000.0, 45000.0, 75780.0, 40000.0, 40000.0, 40000.0)
            case _  => List(0.0)
        }).map(Row(_))

        val df = getWKTRowsDf(mc).orderBy("id")
        val results = df.select(st_xmax(col("wkt")))

        checkAnswer(results, expected)

        df.createOrReplaceTempView("source")
        val sqlResults = spark.sql("select st_xmax(wkt) from source")

        checkAnswer(sqlResults, expected)

    }

    def yMinBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

        val expected = (indexSystem match {
            case H3IndexSystem  => List(10.0, 0.0, 10.0, 5.0, 35.18937, 10.0, 10.0, 10.0)
            case BNGIndexSystem => List(10000.0, 0.0, 10000.0, 5000.0, 35189, 10000.0, 10000.0, 10000.0)
            case _ =>  List(0.0)
        }).map(Row(_))

        val df = getWKTRowsDf(mc).orderBy("id")
        val results = df.select(st_ymin(col("wkt")))

        checkAnswer(results, expected)

        df.createOrReplaceTempView("data")
        val sqlResults = spark.sql("select st_ymin(wkt) from data")

        checkAnswer(sqlResults, expected)
    }

    def yMaxBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

        val expected = (mc.getIndexSystem match {
            case H3IndexSystem  => List(40.0, 2.0, 110.0, 60.0, 35.18937, 40.0, 40.0, 40.0)
            case BNGIndexSystem => List(40000.0, 2000.0, 110000.0, 60000.0, 35189, 40000.0, 40000.0, 40000.0)
            case _  => List(0.0)
        }).map(Row(_))

        val df = getWKTRowsDf(mc).orderBy("id")
        val results = df.select(st_ymax(col("wkt")))

        checkAnswer(results, expected)

        df.createOrReplaceTempView("source")
        val sqlResults = spark.sql("select st_ymax(wkt) from source")

        checkAnswer(sqlResults, expected)

    }

    def xMinCodegen(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

        val df = getWKTRowsDf(mc).orderBy("id")
        val results = df.select(st_xmin(col("wkt")))

        val queryExecution = results.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)
    }

    def xMaxCodegen(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

        val df = getWKTRowsDf(mc).orderBy("id")
        val results = df.select(st_xmax(col("wkt")))

        val queryExecution = results.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)
    }

    def yMinCodegen(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

        val df = getWKTRowsDf(mc).orderBy("id")
        val results = df.select(st_ymin(col("wkt")))

        val queryExecution = results.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)
    }

    def yMaxCodegen(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

        val df = getWKTRowsDf(mc).orderBy("id")
        val results = df.select(st_ymax(col("wkt")))

        val queryExecution = results.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)
    }

    def makeCopy(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val df = getWKTRowsDf(mc).orderBy("id")
        val expr = ST_MinMaxXYZ(df.col("wkt").expr, geometryAPI.name, "X", "MAX")
        noException should be thrownBy expr.makeCopy(Array(df.col("wkt").expr))
    }

}
