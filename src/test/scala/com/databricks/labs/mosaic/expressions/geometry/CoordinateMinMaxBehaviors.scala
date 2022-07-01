package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.getWKTRowsDf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions.col

trait CoordinateMinMaxBehaviors { this: AnyFlatSpec =>

    def xMin(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val df = getWKTRowsDf(mc).orderBy("id")
        val results = df
            .select(st_xmin(col("wkt")))
            .as[Double]
            .collect()

        val expected = mc.getIndexSystem match {
            case H3IndexSystem => List(10.0, 0.0, 10.0, 10.0, -75.78033, 10.0, 10.0, 10.0)
            case BNGIndexSystem => List(10000.0, 0.0, 10000.0, 10000.0, 75780.0, 10000.0, 10000.0, 10000.0)
        }

        results.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        df.createOrReplaceTempView("source")
        val sqlResults = spark
            .sql("select st_xmin(wkt) from source")
            .as[Double]
            .collect()

        sqlResults.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def xMinCodegen(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val df = getWKTRowsDf(mc).orderBy("id")
        val result = df
            .select(st_xmin(col("wkt")))
            .as[Double]

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)
    }

    def xMax(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val df = getWKTRowsDf(mc).orderBy("id")
        val results = df.select(st_xmax(col("wkt"))).as[Double].collect()

        val expected = mc.getIndexSystem match {
            case H3IndexSystem => List(40.0, 2.0, 110.0, 45.0, -75.78033, 40.0, 40.0, 40.0)
            case BNGIndexSystem => List(40000.0, 2000.0, 110000.0, 45000.0, 75780.0, 40000.0, 40000.0, 40000.0)
        }

        results.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        df.createOrReplaceTempView("source")
        val sqlResults = spark
            .sql("select st_xmax(wkt) from source")
            .as[Double]
            .collect()

        sqlResults.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def xMaxCodegen(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val df = getWKTRowsDf(mc).orderBy("id")
        val result = df
            .select(st_xmax(col("wkt")))
            .as[Double]

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)
    }

    def yMin(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val df = getWKTRowsDf(mc).orderBy("id")
        val results = df.select(st_ymin(col("wkt"))).as[Double].collect()

        val expected = mc.getIndexSystem match {
            case H3IndexSystem => List(10.0, 0.0, 10.0, 5.0, 35.18937, 10.0, 10.0, 10.0)
            case BNGIndexSystem => List(10000.0, 0.0, 10000.0, 5000.0, 35189, 10000.0, 10000.0, 10000.0)
        }

        results.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        df.createOrReplaceTempView("source")
        val sqlResults = spark
            .sql("select st_ymin(wkt) from source")
            .as[Double]
            .collect()

        sqlResults.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def yMinCodegen(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val df = getWKTRowsDf(mc).orderBy("id")
        val result = df
            .select(st_ymin(col("wkt")))
            .as[Double]

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)
    }

    def yMax(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val df = getWKTRowsDf(mc).orderBy("id")
        val results = df.select(st_ymax(col("wkt"))).as[Double].collect()

        val expected = mc.getIndexSystem match {
            case H3IndexSystem => List(40.0, 2.0, 110.0, 60.0, 35.18937, 40.0, 40.0, 40.0)
            case BNGIndexSystem => List(40000.0, 2000.0, 110000.0, 60000.0, 35189, 40000.0, 40000.0, 40000.0)
        }

        results.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        df.createOrReplaceTempView("source")
        val sqlResults = spark
            .sql("select st_ymax(wkt) from source")
            .as[Double]
            .collect()

        sqlResults.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def yMaxCodegen(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val df = getWKTRowsDf(mc).orderBy("id")
        val result = df
            .select(st_ymax(col("wkt")))
            .as[Double]

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)
    }

}
