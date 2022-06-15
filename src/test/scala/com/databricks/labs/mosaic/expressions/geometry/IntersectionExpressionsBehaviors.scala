package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import com.databricks.labs.mosaic.test.mocks._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions._

trait IntersectionExpressionsBehaviors { this: AnyFlatSpec =>

    def intersects(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

        val left = boroughs
            .select(
              col("id").alias("left_id"),
              mosaic_explode(col("wkt"), resolution).alias("left_index"),
              col("wkt").alias("left_wkt")
            )

        val right = boroughs
            .select(
              col("id"),
              st_translate(col("wkt"), sqrt(st_area(col("wkt")) * rand() * 0.1), sqrt(st_area(col("wkt")) * rand() * 0.1)).alias("wkt")
            )
            .select(
              col("id").alias("right_id"),
              mosaic_explode(col("wkt"), resolution).alias("right_index"),
              col("wkt").alias("right_wkt")
            )

        val result = left
            .join(
              right,
              col("left_index.index_id") === col("right_index.index_id")
            )
            .groupBy(
              "left_id",
              "right_id"
            )
            .agg(
              st_intersects_aggregate(col("left_index"), col("right_index")).alias("agg_intersects"),
              first("left_wkt").alias("left_wkt"),
              first("right_wkt").alias("right_wkt")
            )
            .withColumn(
              "flat_intersects",
              st_intersects(col("left_wkt"), col("right_wkt"))
            )
            .withColumn(
              "comparison",
              col("flat_intersects") === col("agg_intersects")
            )

        result.select("comparison").collect().map(_.getBoolean(0)).forall(identity) shouldBe true

        left.createOrReplaceTempView("left")
        right.createOrReplaceTempView("right")

        val result2 = spark.sql("""
                                  |SELECT ST_INTERSECTS_AGGREGATE(LEFT_INDEX, RIGHT_INDEX)
                                  |FROM LEFT
                                  |INNER JOIN RIGHT ON LEFT_INDEX.INDEX_ID == RIGHT_INDEX.INDEX_ID
                                  |GROUP BY LEFT_ID, RIGHT_ID
                                  |""".stripMargin)

        result2.collect().length should be > 0

    }

    def intersectsCodegen(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val result = mocks
            .getWKTRowsDf(mc)
            .select(st_intersects($"wkt", $"wkt"))
            .as[Boolean]

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)
    }

    def intersection(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

        val left = boroughs
            .select(
              col("wkt"),
              col("id").alias("left_id"),
              mosaic_explode(col("wkt"), resolution).alias("left_index"),
              col("wkt").alias("left_wkt")
            )

        val right = boroughs
            .select(
              col("id"),
              st_translate(col("wkt"), sqrt(st_area(col("wkt")) * 0.1), sqrt(st_area(col("wkt")) * 0.1)).alias("wkt")
            )
            .select(
              col("wkt"),
              col("id").alias("right_id"),
              mosaic_explode(col("wkt"), resolution).alias("right_index"),
              col("wkt").alias("right_wkt")
            )

        val result = left
            .drop("wkt")
            .join(
              right,
              col("left_index.index_id") === col("right_index.index_id")
            )
            .groupBy(
              "left_id",
              "right_id"
            )
            .agg(
              st_intersection_aggregate(col("left_index"), col("right_index")).alias("agg_intersection"),
              first("left_wkt").alias("left_wkt"),
              first("right_wkt").alias("right_wkt")
            )
            .withColumn("agg_area", st_area(col("agg_intersection")))
            .withColumn("flat_intersection", st_intersection(col("left_wkt"), col("right_wkt")))
            .withColumn("flat_area", st_area(col("flat_intersection")))
            .withColumn("comparison", abs(col("agg_area") - col("flat_area")) <= lit(1e-8)) // ESRI Spatial tolerance

        result.select("comparison").collect().map(_.getBoolean(0)).forall(identity) shouldBe true
    }

    def intersectionCodegen(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val result = mocks
            .getWKTRowsDf(mc)
            .select(st_intersection($"wkt", $"wkt"))
            .as[String]

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)
    }

}
