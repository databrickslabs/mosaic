package com.databricks.labs.gbx.vectorx.jts

import org.locationtech.jts.geom.Coordinate
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.collection.mutable

class JTSTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {}

    override def afterAll(): Unit = {}

    test("JTS should create geometries") {
        val point = JTS.point(1.0, 2.0)
        JTS.toWKT(point) shouldBe "POINT (1 2)"

        val point2 = JTS.point(new Coordinate(3.0, 4.0))
        JTS.toWKT(point2) shouldBe "POINT (3 4)"

        val polygon = JTS.polygonFromPoints(
          Array(
            JTS.point(0.0, 0.0),
            JTS.point(1.0, 0.0),
            JTS.point(1.0, 1.0),
            JTS.point(0.0, 1.0),
            JTS.point(0.0, 0.0)
          )
        )
        JTS.toWKT(polygon) shouldBe "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"

        val polygon2 = JTS.polygonFromCoords(
          Array(
            new Coordinate(0.0, 0.0),
            new Coordinate(2.0, 0.0),
            new Coordinate(2.0, 2.0),
            new Coordinate(0.0, 2.0),
            new Coordinate(0.0, 0.0)
          )
        )
        JTS.toWKT(polygon2) shouldBe "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))"

        val polygon3 = JTS.polygonFromXYs(
          Array((0.0, 0.0), (3.0, 0.0), (3.0, 3.0), (0.0, 3.0), (0.0, 0.0))
        )
        JTS.toWKT(polygon3) shouldBe "POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0))"

        val mpolygon = JTS.multiPolygonFromXYs(
          Array(
            Array((0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)),
            Array((2.0, 2.0), (3.0, 2.0), (3.0, 3.0), (2.0, 3.0), (2.0, 2.0))
          )
        )
        JTS.toWKT(
          mpolygon
        ) shouldBe "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)), ((2 2, 3 2, 3 3, 2 3, 2 2)))"

        val coords = JTS.coordinatesFromXYs(1.0, 2.0)
        noException should be thrownBy coords.toString

        val line = JTS.lineStringXYs(mutable.Buffer((0.0, 0.0), (1.0, 1.0), (2.0, 2.0)))
        JTS.toWKT(line) shouldBe "LINESTRING (0 0, 1 1, 2 2)"

        val mpoint = JTS.multiPoint(
          Array(
            JTS.point(1.0, 1.0),
            JTS.point(2.0, 2.0),
            JTS.point(3.0, 3.0)
          )
        )
        JTS.toWKT(mpoint) shouldBe "MULTIPOINT ((1 1), (2 2), (3 3))"

        val mline = JTS.multiLineString(
          Array(
            JTS.lineStringXYs(mutable.Buffer((0.0, 0.0), (1.0, 1.0))),
            JTS.lineStringXYs(mutable.Buffer((2.0, 2.0), (3.0, 3.0)))
          )
        )
        JTS.toWKT(mline) shouldBe "MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))"

        val ap = JTS.anyPoint(polygon)
        noException should be thrownBy ap.toString

    }

}
