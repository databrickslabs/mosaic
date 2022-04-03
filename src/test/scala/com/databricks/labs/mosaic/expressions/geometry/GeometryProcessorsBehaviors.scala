package com.databricks.labs.mosaic.expressions.geometry

import scala.collection.immutable

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.io.{WKTReader, WKTWriter}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions.lit

trait GeometryProcessorsBehaviors { this: AnyFlatSpec =>

    val wktReader = new WKTReader()
    val wktWriter = new WKTWriter()
    val geomFactory = new GeometryFactory()

    def lengthCalculation(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        // TODO break into two for line segment vs. polygons

        val referenceGeoms: immutable.Seq[MosaicGeometry] =
            mocks.wkt_rows.map(_(1).asInstanceOf[String]).map(mc.getGeometryAPI.geometry(_, "WKT"))

        val expected = referenceGeoms.map(_.getLength)
        val result = mocks.getWKTRowsDf
            .select(st_length($"wkt"))
            .as[Double]
            .collect()

        result.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        val result2 = mocks.getWKTRowsDf
            .select(st_perimeter($"wkt"))
            .as[Double]
            .collect()

        result2.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        mocks.getWKTRowsDf.createOrReplaceTempView("source")

        val sqlResult = spark
            .sql("select st_length(wkt) from source")
            .as[Double]
            .collect()

        sqlResult.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        val sqlResult2 = spark
            .sql("select st_perimeter(wkt) from source")
            .as[Double]
            .collect()

        sqlResult2.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def lengthCalculationCodegen(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        // TODO break into two for line segment vs. polygons

        val result = mocks.getWKTRowsDf
            .select(st_length($"wkt"))
            .as[Double]

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)
    }

    def areaCalculation(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val referenceGeoms: immutable.Seq[MosaicGeometry] =
            mocks.wkt_rows.map(_(1).asInstanceOf[String]).map(mc.getGeometryAPI.geometry(_, "WKT"))

        val expected = referenceGeoms.map(_.getArea)
        val result = mocks.getWKTRowsDf
            .select(st_area($"wkt"))
            .as[Double]
            .collect()

        result.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        mocks.getWKTRowsDf.createOrReplaceTempView("source")

        val sqlResult = spark
            .sql("select st_area(wkt) from source")
            .as[Double]
            .collect()

        sqlResult.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def areaCodegen(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val result = mocks.getWKTRowsDf
            .select(st_area($"wkt"))

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)
    }

    def centroid2DCalculation(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val referenceGeoms: immutable.Seq[MosaicGeometry] =
            mocks.wkt_rows.map(_(1).asInstanceOf[String]).map(mc.getGeometryAPI.geometry(_, "WKT"))

        val expected = referenceGeoms.map(_.getCentroid.coord).map(c => (c.x, c.y))
        val result = mocks.getWKTRowsDf
            .select(st_centroid2D($"wkt").alias("coord"))
            .selectExpr("coord.*")
            .as[(Double, Double)]
            .collect()

        result.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        mocks.getWKTRowsDf.createOrReplaceTempView("source")

        val sqlResult = spark
            .sql("""with subquery (
                   | select st_centroid2D(wkt) as coord from source
                   |) select coord.* from subquery""".stripMargin)
            .as[(Double, Double)]
            .collect()

        sqlResult.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def centroid2DCodegen(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val result = mocks.getWKTRowsDf
            .select(st_centroid2D($"wkt").alias("coord"))
            .selectExpr("coord.*")

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)
    }

    def distanceCalculation(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val referenceGeoms: immutable.Seq[MosaicGeometry] =
            mocks.wkt_rows.map(_(1).asInstanceOf[String]).map(mc.getGeometryAPI.geometry(_, "WKT"))

        val coords = referenceGeoms.head.getShells
        val pointsWKT = coords.map(_.toWKT)
        val pointWKTCompared = pointsWKT.zip(pointsWKT.tail)
        val expected = coords.zip(coords.tail).map({ case (a, b) => a.distance(b) })

        val df = pointWKTCompared.toDF("leftGeom", "rightGeom")

        val result = df.select(st_distance($"leftGeom", $"rightGeom")).as[Double].collect()

        result.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        df.createOrReplaceTempView("source")
        val sqlResult = spark.sql("select st_distance(leftGeom, rightGeom) from source").as[Double].collect()

        sqlResult.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def distanceCodegen(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val referenceGeoms: immutable.Seq[MosaicGeometry] =
            mocks.wkt_rows.map(_(1).asInstanceOf[String]).map(mc.getGeometryAPI.geometry(_, "WKT"))

        val coords = referenceGeoms.head.getShells
        val df = coords.map(_.toWKT).toDF("point")

        val result = df
            .withColumnRenamed("point", "pointLeft")
            .crossJoin(df.withColumnRenamed("point", "pointRight"))
            .select(st_distance($"pointLeft", st_asbinary($"pointRight")))

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)
    }

    def polygonContains(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val poly = """POLYGON ((10 10, 110 10, 110 110, 10 110, 10 10),
                     | (20 20, 20 30, 30 30, 30 20, 20 20),
                     | (40 20, 40 30, 50 30, 50 20, 40 20))""".stripMargin.filter(_ >= ' ')

        val rows = List(
          (poly, "POINT (35 25)", true),
          (poly, "POINT (25 25)", false)
        )

        val results = rows
            .toDF("leftGeom", "rightGeom", "expected")
            .withColumn("result", st_contains($"leftGeom", $"rightGeom"))
            .where($"expected" === $"result")

        results.count shouldBe 2

    }

    def containsCodegen(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val poly = """POLYGON ((10 10, 110 10, 110 110, 10 110, 10 10),
                     | (20 20, 20 30, 30 30, 30 20, 20 20),
                     | (40 20, 40 30, 50 30, 50 20, 40 20))""".stripMargin.filter(_ >= ' ')

        val rows = List(
          ("POINT (35 25)", true),
          ("POINT (25 25)", false)
        )

        val polygons = List(poly).toDF("leftGeom")
        val points = rows.toDF("rightGeom", "expected")

        val result = polygons
            .crossJoin(points)
            .withColumn("result", st_contains($"leftGeom", $"rightGeom"))
            .where($"expected" === $"result")

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)
    }

    def convexHullGeneration(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val multiPoint = List("MULTIPOINT (-70 35, -80 45, -70 45, -80 35)")
        val expected = List("POLYGON ((-80 35, -80 45, -70 45, -70 35, -80 35))")
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        val results = multiPoint
            .toDF("multiPoint")
            .crossJoin(multiPoint.toDF("other"))
            .withColumn("result", st_convexhull($"multiPoint"))
            .select($"result")
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        results.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

    }

    def convexHullCodegen(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val multiPoint = List("MULTIPOINT (-70 35, -80 45, -70 45, -80 35)").toDF("multiPoint")
        val expected = List("POLYGON ((-80 35, -80 45, -70 45, -70 35, -80 35))").toDF("polygon")

        val result = multiPoint
            .crossJoin(expected)
            .withColumn("result", st_convexhull($"multiPoint"))
            .select(st_asbinary($"result"))

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)
    }

    def transformationsCodegen(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val multiPoint = List("MULTIPOINT (-70 35, -80 45, -70 45, -80 35)")

        val result = multiPoint
            .toDF("multiPoint")
            .crossJoin(multiPoint.toDF("other"))
            .withColumn("result", st_convexhull($"multiPoint"))
            .select($"result")
            .select(
              st_translate(
                st_scale(
                  st_rotate($"result", lit(1.1)),
                  lit(1.1),
                  lit(1.2)
                ),
                lit(1.2),
                lit(1.3)
              )
            )
            .as[String]

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        result.collect().length > 0 shouldBe true
    }

    def bufferCalculation(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val referenceGeoms: immutable.Seq[MosaicGeometry] =
            mocks.wkt_rows.sortBy(_.head.asInstanceOf[Int])
                .map(_(1).asInstanceOf[String])
                .map(mc.getGeometryAPI.geometry(_, "WKT"))

        val expected = referenceGeoms.map(_.buffer(1).getLength)
        val result = mocks.getWKTRowsDf
            .orderBy("id")
            .select(st_length(st_buffer($"wkt", lit(1))))
            .as[Double]
            .collect()

        result.zip(expected).foreach { case (l, r) => math.abs(l - r) should be < 1e-8 }

        mocks.getWKTRowsDf.createOrReplaceTempView("source")

        val sqlResult = spark
            .sql("select id, st_length(st_buffer(wkt, 1)) from source")
            .orderBy("id")
            .drop("id")
            .as[Double]
            .collect()

        sqlResult.zip(expected).foreach { case (l, r) => math.abs(l - r) should be < 1e-8 }
    }

    def bufferCodegen(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val result = mocks.getWKTRowsDf
            .select(st_length(st_buffer($"wkt", lit(1))))

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)
    }

}
