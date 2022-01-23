package com.databricks.mosaic.expressions.geometry

import scala.collection.immutable

import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.io.{WKTReader, WKTWriter}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.SparkSession

import com.databricks.mosaic.core.geometry.MosaicGeometry
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.mocks


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

    def distanceCalculation(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val referenceGeoms: immutable.Seq[MosaicGeometry] =
            mocks.wkt_rows.map(_(1).asInstanceOf[String]).map(mc.getGeometryAPI.geometry(_, "WKT"))

        val coords = referenceGeoms.head.getBoundary
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
            .withColumn("result", st_convexhull($"multiPoint"))
            .select(st_astext($"result"))
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        results.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

    }

}
