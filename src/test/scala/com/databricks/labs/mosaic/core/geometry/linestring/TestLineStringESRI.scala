package com.databricks.labs.mosaic.core.geometry.linestring

import com.databricks.labs.mosaic.core.geometry.point.MosaicPointESRI
import com.databricks.labs.mosaic.core.geometry.polygon.MosaicPolygonESRI
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.catalyst.InternalRow

//noinspection ScalaRedundantCast
class TestLineStringESRI extends AnyFlatSpec {

    "MosaicLineStringESRI" should "throw exceptions for function calls when empty construct is used" in {
        val lineString = new MosaicLineStringESRI()
        an[NullPointerException] should be thrownBy lineString.numPoints
    }

    "MosaicLineStringESRI" should "return Nil for holes and hole points calls." in {
        val lineString = MosaicLineStringESRI.fromWKT("LINESTRING (1 1, 2 2, 3 3)")
        lineString.getHoles shouldEqual Nil
        lineString.getHolePoints shouldEqual Nil
    }

    "MosaicLineStringESRI" should "return seq(this) for shells and flatten calls." in {
        val lineString = MosaicLineStringESRI.fromWKT("LINESTRING (1 1, 2 2, 3 3)")
        lineString.getShells.head shouldEqual lineString
        lineString.flatten.head shouldEqual lineString
    }

    "MosaicLineStringESRI" should "return number of points." in {
        val lineString = MosaicLineStringESRI.fromWKT("LINESTRING (1 1, 2 2, 3 3)")
        lineString.numPoints shouldEqual 3
    }

    "MosaicLineStringESRI" should "read all supported formats" in {
        val lineString = MosaicLineStringESRI.fromWKT("LINESTRING (1 1, 2 2, 3 3)")
        noException should be thrownBy MosaicLineStringESRI.fromWKB(lineString.toWKB)
        noException should be thrownBy MosaicLineStringESRI.fromHEX(lineString.toHEX)
        noException should be thrownBy MosaicLineStringESRI.fromJSON(lineString.toJSON)
        noException should be thrownBy MosaicLineStringESRI.fromInternal(lineString.toInternal.serialize.asInstanceOf[InternalRow])
        lineString.equals(MosaicLineStringESRI.fromWKB(lineString.toWKB)) shouldBe true
        lineString.equals(MosaicLineStringESRI.fromHEX(lineString.toHEX)) shouldBe true
        lineString.equals(MosaicLineStringESRI.fromJSON(lineString.toJSON)) shouldBe true
        lineString.equals(MosaicLineStringESRI.fromInternal(lineString.toInternal.serialize.asInstanceOf[InternalRow])) shouldBe true
    }

    "MosaicLineStringESRI" should "be instantiable from a Seq of MosaicPointESRI" in {
        val lineStringReference = MosaicLineStringESRI.fromWKT("LINESTRING (1 1, 2 2, 3 3)")
        val pointsSeq = Seq("POINT (1 1)", "POINT (2 2)", "POINT (3 3)")
            .map(MosaicLineStringESRI.fromWKT)
            .map(_.asInstanceOf[MosaicPointESRI])
        val lineStringTest = MosaicLineStringESRI.fromSeq(pointsSeq)
        lineStringReference.equals(lineStringTest) shouldBe true
    }

    "MosaicLineStringESRI" should "not fail for empty Seq" in {
        val expected = MosaicLineStringESRI.fromWKT(
            "LINESTRING EMPTY"
        )
        val actual = MosaicLineStringESRI.fromSeq(Seq())
        expected.equals(actual) shouldBe true
    }

    "MosaicLineStringESRI" should "return a Seq of MosaicPointESRI object when calling asSeq" in {
        val lineString = MosaicLineStringESRI.fromWKT("LINESTRING (1 1, 2 2, 3 3)").asInstanceOf[MosaicLineStringESRI]
        val pointsSeqReference = Seq("POINT (1 1)", "POINT (2 2)", "POINT (3 3)")
            .map(MosaicPointESRI.fromWKT)
            .map(_.asInstanceOf[MosaicPointESRI])
        val pointSeqTest = lineString.asSeq.map(_.asInstanceOf[MosaicPointESRI])
        val results = pointsSeqReference
            .zip(pointSeqTest)
            .map { case (a: MosaicPointESRI, b: MosaicPointESRI) => a.equals(b) }
        results should contain only true
    }

    "MosaicLineStringESRI" should "return a Seq of MosaicPointESRI object with the correct SRID when calling asSeq" in {
        val srid = 32632
        val lineString = MosaicLineStringESRI
            .fromWKT("LINESTRING (1 1, 2 2, 3 3)")
            .asInstanceOf[MosaicLineStringESRI]
        lineString.setSpatialReference(srid)
        val pointsSeqReference = Seq("POINT (1 1)", "POINT (2 2)", "POINT (3 3)")
            .map(MosaicPointESRI.fromWKT)
            .map(_.asInstanceOf[MosaicPointESRI])
        pointsSeqReference.foreach(_.setSpatialReference(srid))
        val pointSeqTest = lineString.asSeq.map(_.asInstanceOf[MosaicPointESRI])

        pointSeqTest.map(_.getSpatialReference) should contain only srid

        val results = pointsSeqReference
            .zip(pointSeqTest)
            .map { case (a: MosaicPointESRI, b: MosaicPointESRI) => a.getSpatialReference == b.getSpatialReference }
        results should contain only true
    }

    "MosaicLineStringESRI" should "maintain SRID across operations" in {
        val srid = 32632
        val lineString = MosaicLineStringESRI.fromWKT("LINESTRING (1 1, 2 2, 3 3)").asInstanceOf[MosaicLineStringESRI]
        val anotherPoint = MosaicPointESRI.fromWKT("POINT(1 1)").asInstanceOf[MosaicPointESRI]
        val poly = MosaicPolygonESRI.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")

        lineString.setSpatialReference(srid)

        // MosaicGeometryESRI
        lineString.buffer(2d).getSpatialReference shouldBe srid
        lineString.convexHull.getSpatialReference shouldBe srid
        lineString.getCentroid.getSpatialReference shouldBe srid
        lineString.intersection(poly).getSpatialReference shouldBe srid
        lineString.rotate(45).getSpatialReference shouldBe srid
        lineString.scale(2d, 2d).getSpatialReference shouldBe srid
        lineString.simplify(0.001).getSpatialReference shouldBe srid
        lineString.translate(2d, 2d).getSpatialReference shouldBe srid
        lineString.union(anotherPoint).getSpatialReference shouldBe srid

        // MosaicLineString
        lineString.asSeq.head.getSpatialReference shouldBe srid
        lineString.flatten.head.getSpatialReference shouldBe srid
        lineString.getShells.head.getSpatialReference shouldBe srid

        // MosaicLineStringESRI
        lineString.getBoundary.getSpatialReference shouldBe srid
        lineString.getShellPoints.head.head.getSpatialReference shouldBe srid
        lineString.mapXY({ (x: Double, y: Double) => (x * 2, y / 2) }).getSpatialReference shouldBe srid
    }

    "MosaicPolygonESRI" should "correctly apply CRS transformation" in {
        val sridSource = 4326
        val sridTarget = 27700
        val testLine = MosaicLineStringESRI
            .fromWKT(
              "LINESTRING(-0.1367293 51.5166525, -0.1370977 51.517082, -0.1380077 51.5186537, -0.1375356 51.518824)"
            )
            .asInstanceOf[MosaicLineStringESRI]
        testLine.setSpatialReference(sridSource)
        val expectedResult = MosaicLineStringESRI
            .fromWKT(
              "LINESTRING(529382.90 181393.19, 529356.12 181440.30, 529288.54 181613.47, 529320.81 181633.24)"
            )
            .asInstanceOf[MosaicLineStringESRI]
        val testResult = testLine.transformCRSXY(sridTarget).asInstanceOf[MosaicLineStringESRI]
        expectedResult.distance(testResult) should be < 0.001d
    }

}
