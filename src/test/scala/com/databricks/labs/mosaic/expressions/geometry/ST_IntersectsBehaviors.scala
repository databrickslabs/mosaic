package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import com.databricks.labs.mosaic.test.mocks.getBoroughs
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.{be, convertToAnyShouldWrapper, noException}

trait ST_IntersectsBehaviors extends QueryTest {

    def intersectsBehaviour(indexSystem: IndexSystem, geometryAPI: GeometryAPI, resolution: Int): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

        val left = boroughs
            .select(
              col("id").alias("left_id"),
              grid_tessellateexplode(col("wkt"), resolution).alias("left_index"),
              col("wkt").alias("left_wkt")
            )

        val right = boroughs
            .select(
              col("id"),
              st_translate(col("wkt"), sqrt(st_area(col("wkt")) * rand() * 0.1), sqrt(st_area(col("wkt")) * rand() * 0.1)).alias("wkt")
            )
            .select(
              col("id").alias("right_id"),
              grid_tessellateexplode(col("wkt"), resolution).alias("right_index"),
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
              st_intersects_agg(col("left_index"), col("right_index")).alias("agg_intersects"),
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
                                  |SELECT ST_INTERSECTS_AGG(LEFT_INDEX, RIGHT_INDEX)
                                  |FROM LEFT
                                  |INNER JOIN RIGHT ON LEFT_INDEX.INDEX_ID == RIGHT_INDEX.INDEX_ID
                                  |GROUP BY LEFT_ID, RIGHT_ID
                                  |""".stripMargin)

        result2.collect().length should be > 0

        noException should be thrownBy st_intersects_agg(lit("POLYGON (1 1, 2 2, 3 3, 1 1)"), lit("POLYGON (1 1, 2 2, 3 3, 1 1)"))
    }

    def intersectsAggBehaviour(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val indexID1 = if (indexSystem == H3IndexSystem) 608726199203528703L else 10000731741640L
        val indexID2 = if (indexSystem == H3IndexSystem) 608826199220316919L else 10000931541660L
        val indexPolygon1 = indexSystem.indexToGeometry(indexID1, geometryAPI)
        val indexPolygon2 = indexSystem.indexToGeometry(indexID2, geometryAPI)
        val indexPolygonShell1 = indexPolygon1.getShellPoints
        val indexPolygonShell2 = indexPolygon2.getShellPoints
        val chipShell1 = indexPolygonShell1.head.zipWithIndex.filter(_._2 != 2).map(_._1)
        val chipShell2 = indexPolygonShell2.head.zipWithIndex.filter(_._2 != 2).map(_._1)
        val indexChip1 = geometryAPI.geometry(points = chipShell1, GeometryTypeEnum.POLYGON)
        val indexChip2 = geometryAPI.geometry(points = chipShell2, GeometryTypeEnum.POLYGON)

        val matchRows = List(
          List(1L, true, indexID1, indexPolygon1.toWKB, true, indexID1, indexPolygon1.toWKB),
          List(2L, false, indexID1, indexChip1.toWKB, true, indexID1, indexPolygon1.toWKB),
          List(3L, true, indexID2, indexPolygon2.toWKB, false, indexID2, indexChip2.toWKB),
          List(4L, false, indexID2, indexChip2.toWKB, false, indexID2, indexChip2.toWKB),
          List(5L, false, indexID2, indexChip1.toWKB, false, indexID2, indexChip2.toWKB)
        )
        val rows = matchRows.map { x => Row(x: _*) }
        val rdd = spark.sparkContext.makeRDD(rows)
        val schema = StructType(
          Seq(
            StructField("row_id", LongType),
            StructField("left_is_core", BooleanType),
            StructField("left_index_id", LongType),
            StructField("left_wkb", BinaryType),
            StructField("right_is_core", BooleanType),
            StructField("right_index_id", LongType),
            StructField("right_wkb", BinaryType)
          )
        )

        val chips = spark
            .createDataFrame(rdd, schema)
            .select(
              col("row_id"),
              struct(col("left_is_core"), col("left_index_id"), col("left_wkb")).alias("left_index"),
              struct(col("right_is_core"), col("right_index_id"), col("right_wkb")).alias("right_index")
            )

        val results = chips
            .groupBy("row_id")
            .agg(st_intersects_agg(col("left_index"), col("right_index")).alias("flag"))

        results.select("flag").as[Boolean].collect() should contain theSameElementsAs Seq(true, true, true, true, false)
    }

    def selfIntersectsBehaviour(indexSystem: IndexSystem, geometryAPI: GeometryAPI, resolution: Int): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

        val boroughs: DataFrame = getBoroughs(mc).limit(1)

        val left = boroughs
            .select(
              col("wkt"),
              col("id").alias("left_id"),
              grid_tessellateexplode(col("wkt"), resolution).alias("left_index"),
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
              st_intersects_agg(col("left_index"), col("right_index")).alias("agg_intersects"),
              first("left_wkt").alias("left_wkt"),
              first("right_wkt").alias("right_wkt")
            )
            .withColumn("flat_intersects", st_intersects(col("left_wkt"), col("right_wkt")))
            .withColumn("comparison", col("flat_intersects") === col("agg_intersects"))

        result.select("comparison").collect().map(_.getBoolean(0)).forall(identity) shouldBe true
    }

    def intersectsCodegen(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val result = mocks
            .getWKTRowsDf()
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
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(spark)

        val stIntersects =
            ST_Intersects(lit("POLYGON (1 1, 2 2, 3 3, 4 4, 1 1)").expr, lit("POLYGON (1 2, 2 2, 3 3, 4 2, 1 2)").expr, mc.expressionConfig)

        stIntersects.left shouldEqual lit("POLYGON (1 1, 2 2, 3 3, 4 4, 1 1)").expr
        stIntersects.right shouldEqual lit("POLYGON (1 2, 2 2, 3 3, 4 2, 1 2)").expr

        stIntersects.dataType shouldEqual BooleanType
        noException should be thrownBy stIntersects.makeCopy(stIntersects.children.toArray)

    }

}
