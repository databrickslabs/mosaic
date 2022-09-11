package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum
import com.databricks.labs.mosaic.expressions.index.MosaicFill
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import com.databricks.labs.mosaic.test.mocks.getBoroughs
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
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

    def intersectionAggBehaviour(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val indexID1 = if (indexSystem == H3IndexSystem) 608726199203528703L else 10000731741640L
        val indexID2 = if (indexSystem == H3IndexSystem) 608726199220305919L else 10000731541660L
        val indexPolygon1 = indexSystem.indexToGeometry(indexID1, geometryAPI)
        val indexPolygon2 = indexSystem.indexToGeometry(indexID2, geometryAPI)
        val indexPolygonShell1 = indexPolygon1.getShellPoints
        val indexPolygonShell2 = indexPolygon2.getShellPoints
        val chipShell1 = indexPolygonShell1.head.zipWithIndex.filter(_._2 != 2).map(_._1)
        val chipShell2 = indexPolygonShell2.head.zipWithIndex.filter(_._2 != 2).map(_._1)
        val indexChip1 = geometryAPI.geometry(points = chipShell1, GeometryTypeEnum.POLYGON)
        val indexChip2 = geometryAPI.geometry(points = chipShell2, GeometryTypeEnum.POLYGON)

        val chipsRows = List(
          List(1L, true, indexID1, indexPolygon1.toWKB),
          List(1L, true, indexID2, indexPolygon2.toWKB),
          List(1L, false, indexID1, indexChip1.toWKB),
          List(1L, false, indexID2, indexChip2.toWKB)
        )
        val rows = chipsRows.map { x => Row(x: _*) }
        val rdd = spark.sparkContext.makeRDD(rows)
        val schema = StructType(
          Seq(
            StructField("row_id", LongType),
            StructField("is_core", BooleanType),
            StructField("index_id", LongType),
            StructField("wkb", BinaryType)
          )
        )

        val chips =
            spark.createDataFrame(rdd, schema).select(col("row_id"), struct(col("is_core"), col("index_id"), col("wkb")).alias("index"))
        val left = chips.select(
          col("row_id").alias("left_row_id"),
          col("index").alias("left_index")
        )
        val right = chips.select(
          col("row_id").alias("right_row_id"),
          col("index").alias("right_index")
        )

        val results = left
            .join(
              right,
              col("left_index.index_id") === col("right_index.index_id")
            )
            .groupBy("left_row_id")
            .agg(st_intersection_aggregate(col("left_index"), col("right_index")).alias("geom"))
            .withColumn("area", st_area(col("geom")))

        (results.select("area").as[Double].collect().head -
            indexPolygon1.union(indexChip1).union(indexPolygon2).union(indexChip2).getArea) should be < 10e-8
    }

    def intersectionMosaicBehaviour(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val indexID1 = if (indexSystem == H3IndexSystem) 608726199203528703L else 10000731741640L
        val indexID2 = if (indexSystem == H3IndexSystem) 608726199220305919L else 10000731541660L
        val indexPolygon1 = indexSystem.indexToGeometry(indexID1, geometryAPI)
        val indexPolygon2 = indexSystem.indexToGeometry(indexID2, geometryAPI)
        val indexPolygonShell1 = indexPolygon1.getShellPoints
        val indexPolygonShell2 = indexPolygon2.getShellPoints
        val chipShell1 = indexPolygonShell1.head.zipWithIndex.filter(_._2 != 2).map(_._1)
        val chipShell2 = indexPolygonShell2.head.zipWithIndex.filter(_._2 != 2).map(_._1)
        val indexChip1 = geometryAPI.geometry(points = chipShell1, GeometryTypeEnum.POLYGON)
        val indexChip2 = geometryAPI.geometry(points = chipShell2, GeometryTypeEnum.POLYGON)

        val chipsRows = List(
          List(1L, true, indexID1, indexPolygon1.toWKB),
          List(1L, true, indexID2, indexPolygon2.toWKB),
          List(1L, false, indexID1, indexChip1.toWKB),
          List(1L, false, indexID2, indexChip2.toWKB)
        )
        val rows = chipsRows.map { x => Row(x: _*) }
        val rdd = spark.sparkContext.makeRDD(rows)
        val schema = StructType(
          Seq(
            StructField("row_id", LongType),
            StructField("is_core", BooleanType),
            StructField("index_id", LongType),
            StructField("wkb", BinaryType)
          )
        )

        val chips =
            spark.createDataFrame(rdd, schema).select(col("row_id"), struct(col("is_core"), col("index_id"), col("wkb")).alias("index"))
        val left = chips.groupBy("row_id").agg(collect_list("index").alias("index")).as("left")
        val right = chips.groupBy("row_id").agg(collect_list("index").alias("index")).as("right")

        val results = left
            .join(
              right,
              col("left.row_id") === col("right.row_id")
            )
            .withColumn("intersection", st_intersection_mosaic(col("left.index"), col("right.index")))
            .withColumn("area", st_area(col("intersection")))

        (results.select("area").as[Double].collect().head -
            indexPolygon1.union(indexChip1).union(indexPolygon2).union(indexChip2).getArea) should be < 10e-8
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
        import mc.functions._

        val stIntersection =
            ST_Intersection(lit("POLYGON ((3 1, 4 4, 2 4, 1 2, 3 1))").expr, lit("POLYGON ((3 1, 7 7, 2 4, 1 2, 3 1))").expr, geometryAPI.name)

        stIntersection.left shouldEqual lit("POLYGON ((3 1, 4 4, 2 4, 1 2, 3 1))").expr
        stIntersection.right shouldEqual lit("POLYGON ((3 1, 7 7, 2 4, 1 2, 3 1))").expr

        stIntersection.dataType shouldEqual lit("POLYGON ((3 1, 4 4, 2 4, 1 2, 3 1))").expr.dataType
        noException should be thrownBy stIntersection.makeCopy(stIntersection.children.toArray)

        val stIntersectionMosaic = ST_IntersectionMosaic(lit("").expr, lit("").expr, geometryAPI.name, indexSystem.name)
        val leftMosaic = mosaicfill(lit(""), lit(3)).expr.asInstanceOf[MosaicFill].nullSafeEval(
            UTF8String.fromString("POLYGON ((3 1, 4 4, 2 4, 1 2, 3 1))"),
            3,
            true,
        ).asInstanceOf[InternalRow].getArray(0)
        val rightMosaic = mosaicfill(lit(""), lit(3)).expr.asInstanceOf[MosaicFill].nullSafeEval(
            UTF8String.fromString("POLYGON ((3 1, 7 7, 2 4, 1 2, 3 1))"),
            3,
            true,
        ).asInstanceOf[InternalRow].getArray(0)
        val emptyMosaic = mosaicfill(lit(""), lit(3)).expr.asInstanceOf[MosaicFill].nullSafeEval(
            UTF8String.fromString("POLYGON EMPTY"),
            3,
            true,
        ).asInstanceOf[InternalRow].getArray(0)

        noException should be thrownBy stIntersectionMosaic.nullSafeEval(leftMosaic, rightMosaic)
        noException should be thrownBy stIntersectionMosaic.nullSafeEval(leftMosaic, emptyMosaic)
        noException should be thrownBy stIntersectionMosaic.nullSafeEval(emptyMosaic, rightMosaic)

    }

}
