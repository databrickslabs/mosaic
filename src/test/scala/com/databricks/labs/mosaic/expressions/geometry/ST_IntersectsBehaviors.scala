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
import org.apache.spark.sql.types.BooleanType
import org.scalatest.matchers.should.Matchers.{be, convertToAnyShouldWrapper, noException}

trait ST_IntersectsBehaviors extends QueryTest {

    def intersectsBehaviour(indexSystem: IndexSystem, geometryAPI: GeometryAPI, resolution: Int): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

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

    def intersectsCodegen(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

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

    def auxiliaryMethods(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(spark)

        val stIntersects =
            ST_Intersects(lit("POLYGON (1 1, 2 2, 3 3, 4 4, 1 1)").expr, lit("POLYGON (1 2, 2 2, 3 3, 4 2, 1 2)").expr, geometryAPI.name)

        stIntersects.left shouldEqual lit("POLYGON (1 1, 2 2, 3 3, 4 4, 1 1)").expr
        stIntersects.right shouldEqual lit("POLYGON (1 2, 2 2, 3 3, 4 2, 1 2)").expr

        stIntersects.dataType shouldEqual BooleanType
        noException should be thrownBy stIntersects.makeCopy(stIntersects.children.toArray)

    }

}
