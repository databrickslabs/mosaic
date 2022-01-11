package com.databricks.mosaic.jts.h3.expressions.geometry

import scala.collection.immutable

import com.databricks.mosaic.mocks
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.io.{WKTReader, WKTWriter}
import org.scalatest._

import com.databricks.mosaic.core.geometry.{MosaicGeometry, MosaicGeometryJTS}
import com.databricks.mosaic.core.geometry.api.GeometryAPI.JTS
import com.databricks.mosaic.core.geometry.point.MosaicPointJTS
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.test.SparkFunSuite

class TestGeometryProcessors_JTS_H3 extends SparkFunSuite with Matchers {

    val mosaicContext: MosaicContext = MosaicContext.build(H3IndexSystem, JTS)

    import mosaicContext.functions._
    import testImplicits._

    val wktReader = new WKTReader()
    val wktWriter = new WKTWriter()
    val geomFactory = new GeometryFactory()
    val referenceGeoms: immutable.Seq[MosaicGeometry] = mocks.wkt_rows.map(_(1).asInstanceOf[String]).map(MosaicGeometryJTS.fromWKT)

    test("Test length (or perimeter) calculation") {
        mosaicContext.register(spark)
        // TODO break into two for line segment vs. polygons

        val expected = referenceGeoms.map(_.getLength)
        val result = mocks.getWKTRowsDf
            .select(st_length($"wkt"))
            .as[Double]
            .collect()

        result should contain theSameElementsAs expected

        val result2 = mocks.getWKTRowsDf
            .select(st_perimeter($"wkt"))
            .as[Double]
            .collect()

        result2 should contain theSameElementsAs expected

        mocks.getWKTRowsDf.createOrReplaceTempView("source")

        val sqlResult = spark
            .sql("select st_length(wkt) from source")
            .as[Double]
            .collect()

        sqlResult should contain theSameElementsAs expected

        val sqlResult2 = spark
            .sql("select st_perimeter(wkt) from source")
            .as[Double]
            .collect()

        sqlResult2 should contain theSameElementsAs expected
    }

    test("Test area calculation") {
        mosaicContext.register(spark)

        val expected = referenceGeoms.map(_.getArea)
        val result = mocks.getWKTRowsDf
            .select(st_area($"wkt"))
            .as[Double]
            .collect()

        result should contain theSameElementsAs expected

        mocks.getWKTRowsDf.createOrReplaceTempView("source")

        val sqlResult = spark
            .sql("select st_area(wkt) from source")
            .as[Double]
            .collect()

        sqlResult should contain theSameElementsAs expected

    }

    test("Test centroid calculation (2-dimensional)") {
        mosaicContext.register(spark)

        val expected = referenceGeoms.map(_.getCentroid.coord).map(c => (c.x, c.y))
        val result = mocks.getWKTRowsDf
            .select(st_centroid2D($"wkt").alias("coord"))
            .selectExpr("coord.*")
            .as[Tuple2[Double, Double]]
            .collect()

        result should contain theSameElementsAs expected

        mocks.getWKTRowsDf.createOrReplaceTempView("source")

        val sqlResult = spark
            .sql("""with subquery (
                   | select st_centroid2D(wkt) as coord from source
                   |) select coord.* from subquery""".stripMargin)
            .as[Tuple2[Double, Double]]
            .collect()

        sqlResult should contain theSameElementsAs expected
    }

    test("Test distance calculation") {
        mosaicContext.register(spark)

        val coords = referenceGeoms.head.getBoundary
        val pointsWKT = coords.map(_.asInstanceOf[MosaicPointJTS].getGeom).map(MosaicGeometryJTS(_).toWKT)
        val pointWKTCompared = pointsWKT.zip(pointsWKT.tail)
        val expected = coords.zip(coords.tail).map({ case (a, b) => a.distance(b) })

        val df = pointWKTCompared.toDF("leftGeom", "rightGeom")

        val result = df.select(st_distance($"leftGeom", $"rightGeom")).as[Double].collect()

        result should contain allElementsOf expected

        df.createOrReplaceTempView("source")
        val sqlResult = spark.sql("select st_distance(leftGeom, rightGeom) from source").as[Double].collect()

        sqlResult should contain allElementsOf expected

    }

    test("Test polygon contains point") {
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

    test("Test convex hull generation") {
        mosaicContext.register(spark)
        val multiPoint = List("MULTIPOINT (-70 35, -80 45, -70 45, -80 35)")
        val expected = List("POLYGON ((-80 35, -80 45, -70 45, -70 35, -80 35))")

        val results = multiPoint
            .toDF("multiPoint")
            .withColumn("result", st_convexhull($"multiPoint"))
            .select(st_astext($"result"))
            .as[String]
            .collect()

        results should contain allElementsOf expected
    }

}
