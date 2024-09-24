package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum
import com.databricks.labs.mosaic.core.types.ChipType
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
import org.scalatest.matchers.should.Matchers.{an, be, convertToAnyShouldWrapper}

trait ST_IntersectionBehaviors extends QueryTest {

    def intersectionBehaviour(indexSystem: IndexSystem, geometryAPI: GeometryAPI, resolution: Int): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

        val left = boroughs
            .select(
              col("wkt"),
              col("id").alias("left_id"),
              grid_tessellateexplode(col("wkt"), resolution).alias("left_index"),
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
              grid_tessellateexplode(col("wkt"), resolution).alias("right_index"),
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
              st_intersection_agg(col("left_index"), col("right_index")).alias("agg_intersection"),
              first("left_wkt").alias("left_wkt"),
              first("right_wkt").alias("right_wkt")
            )
            .withColumn("agg_area", st_area(col("agg_intersection")))
            .withColumn("flat_intersection", st_intersection(col("left_wkt"), col("right_wkt")))
            .withColumn("flat_area", st_area(col("flat_intersection")))
            .withColumn("comparison", abs(col("agg_area") - col("flat_area")) <= lit(1e-8)) // Spatial tolerance

        result.select("comparison").collect().map(_.getBoolean(0)).forall(identity) shouldBe true
    }

    def intersectionAggBehaviour(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
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
            .agg(st_intersection_agg(col("left_index"), col("right_index")).alias("geom"))
            .withColumn("area", st_area(col("geom")))

        (results.select("area").as[Double].collect().head -
            indexPolygon1.union(indexChip1).union(indexPolygon2).union(indexChip2).getArea) should be < 10e-8

        left.createOrReplaceTempView("left")
        right.createOrReplaceTempView("right")

        val sqlResults = spark.sql(
          """
            |SELECT
            |  left_row_id,
            |  ST_Area(ST_Intersection_Agg(left_index, right_index)) AS area
            |FROM left
            |JOIN right
            |ON left_index.index_id = right_index.index_id
            |GROUP BY left_row_id
            |""".stripMargin
        )

        (sqlResults.select("area").as[Double].collect().head -
            indexPolygon1.union(indexChip1).union(indexPolygon2).union(indexChip2).getArea) should be < 10e-8

        val sqlResults2 = spark.sql(
          """
            |SELECT
            |  left_row_id,
            |  ST_Area(ST_Intersection_Agg(left_index, right_index)) AS area
            |FROM left
            |JOIN right
            |ON left_index.index_id = right_index.index_id
            |GROUP BY left_row_id
            |""".stripMargin
        )

        (sqlResults2.select("area").as[Double].collect().head -
            indexPolygon1.union(indexChip1).union(indexPolygon2).union(indexChip2).getArea) should be < 10e-8

        noException should be thrownBy st_intersection_agg(lit("POLYGON (1 1, 2 2, 3 3, 1 1)"), lit("POLYGON (1 1, 2 2, 3 3, 1 1)"))
    }

    def selfIntersectionBehaviour(indexSystem: IndexSystem, geometryAPI: GeometryAPI, resolution: Int): Unit = {
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
              st_intersection_agg(col("left_index"), col("right_index")).alias("agg_intersection"),
              first("left_wkt").alias("left_wkt"),
              first("right_wkt").alias("right_wkt")
            )
            .withColumn("agg_area", st_area(col("agg_intersection")))
            .withColumn("flat_intersection", st_intersection(col("left_wkt"), col("right_wkt")))
            .withColumn("flat_area", st_area(col("flat_intersection")))
            .withColumn("comparison", abs(col("agg_area") - col("flat_area")) <= lit(1e-8)) // Spatial tolerance

        result.select("comparison").collect().map(_.getBoolean(0)).forall(identity) shouldBe true
    }

    def intersectionCodegen(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val result = mocks
            .getWKTRowsDf()
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
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(spark)

        val stIntersection = ST_Intersection(
          lit("POLYGON (1 1, 2 2, 3 3, 4 4, 1 1)").expr,
          lit("POLYGON (1 2, 2 2, 3 3, 4 2, 1 2)").expr,
          mc.expressionConfig
        )

        stIntersection.left shouldEqual lit("POLYGON (1 1, 2 2, 3 3, 4 4, 1 1)").expr
        stIntersection.right shouldEqual lit("POLYGON (1 2, 2 2, 3 3, 4 2, 1 2)").expr

        stIntersection.dataType shouldEqual lit("POLYGON (1 1, 2 2, 3 3, 4 4, 1 1)").expr.dataType
        noException should be thrownBy stIntersection.makeCopy(stIntersection.children.toArray)

        val stringIDRow = indexSystem match {
            case BNGIndexSystem => InternalRow.fromSeq(Seq(true, UTF8String.fromString("TQ3879"), Array.empty[Byte]))
            case H3IndexSystem  => InternalRow.fromSeq(Seq(true, UTF8String.fromString("8a2a1072b59ffff"), Array.empty[Byte]))
        }
        val longIDRow = indexSystem match {
            case BNGIndexSystem => InternalRow.fromSeq(Seq(true, 1050138790L, Array.empty[Byte]))
            case H3IndexSystem  => InternalRow.fromSeq(Seq(true, 622236750694711295L, Array.empty[Byte]))
        }

        val stIntersectionAgg = ST_IntersectionAgg(null, null, geometryAPI.name, indexSystem, 0, 0)
        noException should be thrownBy stIntersectionAgg.getCellGeom(stringIDRow, ChipType(StringType))
        noException should be thrownBy stIntersectionAgg.getCellGeom(longIDRow, ChipType(LongType))
        an[Error] should be thrownBy stIntersectionAgg.getCellGeom(
          longIDRow,
          new StructType().add("f1", BooleanType).add("index_id", BooleanType)
        )

    }

}
