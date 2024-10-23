package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers.{an, be, convertToAnyShouldWrapper}

trait ST_UnionBehaviors extends QueryTest {

    def unionBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
         val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val left_polygon = List("POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))")
        val right_polygon = List("POLYGON ((15 15, 25 15, 25 25, 15 25, 15 15))")
        val expected = List("POLYGON ((20 15, 20 10, 10 10, 10 20, 15 20, 15 25, 25 25, 25 15, 20 15))")
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        val results = left_polygon
            .toDF("leftPolygon")
            .crossJoin(right_polygon.toDF("rightPolygon"))
            .withColumn("result", st_union($"leftPolygon", $"rightPolygon"))
            .select($"result")
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        results.zip(expected).foreach { case (l, r) => l.equalsTopo(r) shouldEqual true }
    }

    def unionAggBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val polygonRows = List(
          List(1L, "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))"),
          List(1L, "POLYGON ((15 15, 25 15, 25 25, 15 25, 15 15))")
        )
        val rows = polygonRows.map { x => Row(x: _*) }
        val rdd = spark.sparkContext.makeRDD(rows)
        val schema = StructType(
          Seq(
            StructField("row_id", LongType),
            StructField("polygons", StringType)
          )
        )

        val polygons = spark.createDataFrame(rdd, schema)
        val expected = List("POLYGON ((20 15, 20 10, 10 10, 10 20, 15 20, 15 25, 25 25, 25 15, 20 15))")
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        val results = polygons
            .groupBy($"row_id")
            .agg(
              st_union_agg($"polygons").alias("result")
            )
            .select($"result")
            .as[Array[Byte]]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKB"))

        results.zip(expected).foreach { case (l, r) => l.equalsTopo(r) shouldEqual true }
    }

    def unionAggPointsBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val polygonRows = List(
            List(1L, "POINT Z (10 10 10)"),
            List(1L, "POINT Z (15 15 15)")
        )
        val rows = polygonRows.map { x => Row(x: _*) }
        val rdd = spark.sparkContext.makeRDD(rows)
        val schema = StructType(
            Seq(
                StructField("row_id", LongType),
                StructField("polygons", StringType)
            )
        )

        val polygons = spark.createDataFrame(rdd, schema)
        val expected = List("MULTIPOINT ((10 10), (15 15))")
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        val results = polygons
            .groupBy($"row_id")
            .agg(
                st_union_agg($"polygons").alias("result")
            )
            .select($"result")
            .as[Array[Byte]]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKB"))

        results.zip(expected).foreach { case (l, r) => l.equalsTopo(r) shouldEqual true }
    }

    def unionCodegen(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val result = mocks.getWKTRowsDf().select(st_union($"wkt", $"wkt"))

        // Check if code generation was planned
        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan
        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])
        wholeStageCodegenExec.isDefined shouldBe true

        // Check is generated code compiles
        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()
        noException should be thrownBy CodeGenerator.compile(code)

        // Check if invalid code fails code generation
        val stUnion = ST_Union(lit(1).expr, lit(1).expr, mc.expressionConfig)
        val ctx = new CodegenContext
        an[Error] should be thrownBy stUnion.genCode(ctx)
    }

    def auxiliaryMethods(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(spark)

        val stUnion = ST_Union(
          lit("POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))").expr,
          lit("POLYGON ((15 15, 25 15, 25 25, 15 25, 15 15))").expr,
          mc.expressionConfig
        )

        stUnion.left shouldEqual lit("POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))").expr
        stUnion.right shouldEqual lit("POLYGON ((15 15, 25 15, 25 25, 15 25, 15 15))").expr
        stUnion.dataType shouldEqual lit("POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))").expr.dataType
        noException should be thrownBy stUnion.makeCopy(Array(stUnion.left, stUnion.right))
    }

}
