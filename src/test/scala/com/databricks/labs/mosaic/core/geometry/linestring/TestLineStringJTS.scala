package com.databricks.labs.mosaic.core.geometry.linestring

import com.databricks.labs.mosaic.core.geometry.point.MosaicPointJTS
import com.databricks.labs.mosaic.core.geometry.polygon.MosaicPolygonJTS
import org.apache.spark.sql.catalyst.InternalRow
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

//noinspection ScalaRedundantCast
class TestLineStringJTS extends AnyFlatSpec {

    "MosaicLineStringJTS" should "return Nil for holes and hole points calls." in {
        val lineString = MosaicLineStringJTS.fromWKT("LINESTRING (1 1, 2 2, 3 3)")
        lineString.getHoles shouldEqual Nil
        lineString.getHolePoints shouldEqual Nil
    }

    "MosaicLineStringJTS" should "return seq(this) for shells and flatten calls." in {
        val lineString = MosaicLineStringJTS.fromWKT("LINESTRING (1 1, 2 2, 3 3)")
        lineString.getShells.head shouldEqual lineString
        lineString.flatten.head shouldEqual lineString
    }

    "MosaicLineStringJTS" should "return number of points." in {
        val lineString = MosaicLineStringJTS.fromWKT("LINESTRING (1 1, 2 2, 3 3)")
        lineString.numPoints shouldEqual 3
    }

    "MosaicLineStringJTS" should "read all supported formats" in {
        val lineString = MosaicLineStringJTS.fromWKT("LINESTRING (1 1, 2 2, 3 3)")
        noException should be thrownBy MosaicLineStringJTS.fromWKB(lineString.toWKB)
        noException should be thrownBy MosaicLineStringJTS.fromHEX(lineString.toHEX)
        noException should be thrownBy MosaicLineStringJTS.fromJSON(lineString.toJSON)
        noException should be thrownBy MosaicLineStringJTS.fromInternal(lineString.toInternal.serialize.asInstanceOf[InternalRow])
        lineString.equals(MosaicLineStringJTS.fromWKB(lineString.toWKB)) shouldBe true
        lineString.equals(MosaicLineStringJTS.fromHEX(lineString.toHEX)) shouldBe true
        lineString.equals(MosaicLineStringJTS.fromJSON(lineString.toJSON)) shouldBe true
        lineString.equals(MosaicLineStringJTS.fromInternal(lineString.toInternal.serialize.asInstanceOf[InternalRow])) shouldBe true
    }

    "MosaicLineStringJTS" should "be instantiable from a Seq of MosaicPointJTS" in {
        val lineStringReference = MosaicLineStringJTS.fromWKT("LINESTRING (1 1, 2 2, 3 3)")
        val pointsSeq = Seq("POINT (1 1)", "POINT (2 2)", "POINT (3 3)")
            .map(MosaicLineStringJTS.fromWKT)
            .map(_.asInstanceOf[MosaicPointJTS])
        val lineStringTest = MosaicLineStringJTS.fromSeq(pointsSeq)
        lineStringReference.equals(lineStringTest) shouldBe true
    }

    "MosaicLineStringJTS" should "not fail for empty Seq" in {
        val expected = MosaicLineStringJTS.fromWKT(
          "LINESTRING EMPTY"
        )
        val actual = MosaicLineStringJTS.fromSeq(Seq[MosaicLineStringJTS]())
        expected.equals(actual) shouldBe true
    }

    "MosaicLineStringJTS" should "return a Seq of MosaicPointJTS object when calling asSeq" in {
        val lineString = MosaicLineStringJTS.fromWKT("LINESTRING (1 1, 2 2, 3 3)").asInstanceOf[MosaicLineStringJTS]
        val pointsSeqReference = Seq("POINT (1 1)", "POINT (2 2)", "POINT (3 3)")
            .map(MosaicPointJTS.fromWKT)
            .map(_.asInstanceOf[MosaicPointJTS])
        val pointSeqTest = lineString.asSeq.map(_.asInstanceOf[MosaicPointJTS])
        val results = pointsSeqReference
            .zip(pointSeqTest)
            .map { case (a: MosaicPointJTS, b: MosaicPointJTS) => a.equals(b) }
        results should contain only true
    }

    "MosaicLineStringJTS" should "return a Seq of MosaicPointJTS object with the correct SRID when calling asSeq" in {
        val srid = 32632
        val lineString = MosaicLineStringJTS
            .fromWKT("LINESTRING (1 1, 2 2, 3 3)")
            .asInstanceOf[MosaicLineStringJTS]
        lineString.setSpatialReference(srid)
        val pointsSeqReference = Seq("POINT (1 1)", "POINT (2 2)", "POINT (3 3)")
            .map(MosaicPointJTS.fromWKT)
            .map(_.asInstanceOf[MosaicPointJTS])
        pointsSeqReference.foreach(_.setSpatialReference(srid))
        val pointSeqTest = lineString.asSeq.map(_.asInstanceOf[MosaicPointJTS])

        pointSeqTest.map(_.getSpatialReference) should contain only srid

        val results = pointsSeqReference
            .zip(pointSeqTest)
            .map { case (a: MosaicPointJTS, b: MosaicPointJTS) => a.getSpatialReference == b.getSpatialReference }
        results should contain only true
    }

    "MosaicLineStringJTS" should "maintain SRID across operations" in {
        val srid = 32632
        val lineString = MosaicLineStringJTS.fromWKT("LINESTRING (1 1, 2 2, 3 3)").asInstanceOf[MosaicLineStringJTS]
        val anotherPoint = MosaicPointJTS.fromWKT("POINT(1 1)").asInstanceOf[MosaicPointJTS]
        val poly = MosaicPolygonJTS.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")

        lineString.setSpatialReference(srid)

        // MosaicGeometryJTS
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

        // MosaicLineStringJTS
        lineString.getBoundary.getSpatialReference shouldBe srid
        lineString.getShellPoints.head.head.getSpatialReference shouldBe srid
        lineString.mapXY({ (x: Double, y: Double) => (x * 2, y / 2) }).getSpatialReference shouldBe srid
    }

    "MosaicPolygonJTS" should "correctly apply CRS transformation" in {
        val sridSource = 4326
        val sridTarget = 27700
        val testLine = MosaicLineStringJTS
            .fromWKT(
              "LINESTRING(-0.1367293 51.5166525, -0.1370977 51.517082, -0.1380077 51.5186537, -0.1375356 51.518824)"
            )
            .asInstanceOf[MosaicLineStringJTS]
        testLine.setSpatialReference(sridSource)
        val expectedResult = MosaicLineStringJTS
            .fromWKT(
              "LINESTRING(529382.90 181393.19, 529356.12 181440.30, 529288.54 181613.47, 529320.81 181633.24)"
            )
            .asInstanceOf[MosaicLineStringJTS]
        val testResult = testLine.transformCRSXY(sridTarget).asInstanceOf[MosaicLineStringJTS]
        expectedResult.distance(testResult) should be < 0.001d
    }

}
