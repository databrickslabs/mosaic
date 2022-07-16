package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import com.databricks.labs.mosaic.test.mocks.getBoroughs
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions._
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers.{be, convertToAnyShouldWrapper}

trait ST_IntersectionBehaviors extends QueryTest {

    def intersectionBehaviour(indexSystem: IndexSystem, geometryAPI: GeometryAPI, resolution: Int): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

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

    def selfIntersectionBehaviour(indexSystem: IndexSystem, geometryAPI: GeometryAPI, resolution: Int): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

        val boroughs: DataFrame = getBoroughs(mc).limit(1)

        val left = boroughs
            .select(
              col("wkt"),
              col("id").alias("left_id"),
              mosaic_explode(col("wkt"), resolution).alias("left_index"),
              col("wkt").alias("left_wkt")
            )

        val right = left
            .select(
              col("left_wkt").alias("right_wkt"),
              col("left_id").alias("right_id"),
              col("left_index").alias("right_index")
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

    def intersectionCodegen(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

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

    def auxiliaryMethods(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(spark)

        val stIntersection =
            ST_Intersection(lit("POLYGON (1 1, 2 2, 3 3, 4 4, 1 1)").expr, lit("POLYGON (1 2, 2 2, 3 3, 4 2, 1 2)").expr, geometryAPI.name)

        stIntersection.left shouldEqual lit("POLYGON (1 1, 2 2, 3 3, 4 4, 1 1)").expr
        stIntersection.right shouldEqual lit("POLYGON (1 2, 2 2, 3 3, 4 2, 1 2)").expr

        stIntersection.dataType shouldEqual lit("POLYGON (1 1, 2 2, 3 3, 4 4, 1 1)").expr.dataType
        noException should be thrownBy stIntersection.makeCopy(stIntersection.children.toArray)

    }

}
