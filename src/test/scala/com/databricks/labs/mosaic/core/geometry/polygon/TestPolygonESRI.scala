package com.databricks.labs.mosaic.core.geometry.polygon

import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineStringESRI
import com.databricks.labs.mosaic.core.geometry.point.MosaicPointESRI
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.catalyst.InternalRow

//noinspection ScalaRedundantCast
class TestPolygonESRI extends AnyFlatSpec {

    "MosaicPolygonESRI" should "return Nil for holes and hole points calls." in {
        val point = MosaicPolygonESRI.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")
        point.getHoles shouldEqual Seq(Nil)
        point.getHolePoints shouldEqual Seq(Nil)
    }

    "MosaicPolygonESRI" should "return seq(this) for shells and flatten calls." in {
        val polygon = MosaicPolygonESRI.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")
        val lineString = MosaicLineStringESRI.fromWKT("LINESTRING (0 1,3 0,4 3,0 4,0 1)")
        polygon.getShells.head.equals(lineString) shouldBe true
        polygon.flatten should contain theSameElementsAs Seq(polygon)
    }

    "MosaicPolygonESRI" should "return number of points." in {
        val polygon = MosaicPolygonESRI.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")
        polygon.numPoints shouldEqual 5
    }

    "MosaicPolygonESRI" should "read all supported formats" in {
        val polygon = MosaicPolygonESRI.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")
        noException should be thrownBy MosaicPolygonESRI.fromWKB(polygon.toWKB)
        noException should be thrownBy MosaicPolygonESRI.fromHEX(polygon.toHEX)
        noException should be thrownBy MosaicPolygonESRI.fromJSON(polygon.toJSON)
        noException should be thrownBy MosaicPolygonESRI.fromInternal(polygon.toInternal.serialize.asInstanceOf[InternalRow])
        polygon.equals(MosaicPolygonESRI.fromWKB(polygon.toWKB)) shouldBe true
        polygon.equals(MosaicPolygonESRI.fromHEX(polygon.toHEX)) shouldBe true
        polygon.equals(MosaicPolygonESRI.fromJSON(polygon.toJSON)) shouldBe true
        polygon.equals(MosaicPolygonESRI.fromInternal(polygon.toInternal.serialize.asInstanceOf[InternalRow])) shouldBe true
    }

    "MosaicPolygonESRI" should "be instantiable from a Seq of MosaicPointESRI" in {
        val polygonReference = MosaicPolygonESRI.fromWKT("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")
        val pointSeq = Seq("POINT (30 10)", "POINT (40 40)", "POINT (20 40)", "POINT (10 20)")
            .map(MosaicPointESRI.fromWKT)
            .map(_.asInstanceOf[MosaicPointESRI])
        val polygonTest = MosaicPolygonESRI.fromSeq(pointSeq)
        polygonReference.equals(polygonTest) shouldBe true
    }

    "MosaicPolygonESRI" should "not fail for empty Seq" in {
        val expected = MosaicPolygonESRI.fromWKT(
            "POLYGON EMPTY"
        )
        val actual = MosaicPolygonESRI.fromSeq(Seq())
        expected.equals(actual) shouldBe true
    }

    "MosaicPolygonESRI" should "be instantiable from a Seq of MosaicLineStringESRI" in {
        val polygonReference = MosaicPolygonESRI.fromWKT("POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))")
        val linesSeq = Seq("LINESTRING (35 10, 45 45, 15 40, 10 20)", "LINESTRING (20 30, 35 35, 30 20)")
            .map(MosaicLineStringESRI.fromWKT)
            .map(_.asInstanceOf[MosaicLineStringESRI])
        val polygonTest = MosaicPolygonESRI.fromSeq(linesSeq)
        polygonReference.equals(polygonTest) shouldBe true
    }

    "MosaicPolygonESRI" should "return a Seq of MosaicLineStringESRI object when calling asSeq" in {
        val polygon = MosaicPolygonESRI
            .fromWKT("POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))")
            .asInstanceOf[MosaicPolygonESRI]
        val linesSeqReference = Seq("LINESTRING (35 10, 45 45, 15 40, 10 20, 35 10)", "LINESTRING (20 30, 35 35, 30 20, 20 30)")
            .map(MosaicLineStringESRI.fromWKT)
            .map(_.asInstanceOf[MosaicLineStringESRI])
        val lineSeqTest = polygon.asSeq.map(_.asInstanceOf[MosaicLineStringESRI])
        val results = linesSeqReference
            .zip(lineSeqTest)
            .map { case (a: MosaicLineStringESRI, b: MosaicLineStringESRI) => a.equals(b) }
        results should contain only true
    }

    "MosaicPolygonESRI" should "return a Seq of MosaicLineStringESRI object with the correct SRID when calling asSeq" in {
        val srid = 32632
        val polygon = MosaicPolygonESRI
            .fromWKT("POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))")
            .asInstanceOf[MosaicPolygonESRI]
        polygon.setSpatialReference(srid)
        val linesSeqReference = Seq("LINESTRING (35 10, 45 45, 15 40, 10 20, 35 10)", "LINESTRING (20 30, 35 35, 30 20, 20 30)")
            .map(MosaicLineStringESRI.fromWKT)
            .map(_.asInstanceOf[MosaicLineStringESRI])
        linesSeqReference.foreach(_.setSpatialReference(srid))
        val lineSeqTest = polygon.asSeq.map(_.asInstanceOf[MosaicLineStringESRI])
        lineSeqTest.map(_.getSpatialReference) should contain only srid

        val results = linesSeqReference
            .zip(lineSeqTest)
            .map { case (a: MosaicLineStringESRI, b: MosaicLineStringESRI) => a.getSpatialReference == b.getSpatialReference }
        results should contain only true
    }

    "MosaicPolygonESRI" should "maintain SRID across operations" in {
        val srid = 32632
        val polygon = MosaicPolygonESRI
            .fromWKT("POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))")
            .asInstanceOf[MosaicPolygonESRI]
        val otherPolygon = MosaicPolygonESRI.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")

        polygon.setSpatialReference(srid)

        // MosaicGeometryESRI
        polygon.buffer(2d).getSpatialReference shouldBe srid
        polygon.convexHull.getSpatialReference shouldBe srid
        polygon.getCentroid.getSpatialReference shouldBe srid
        polygon.intersection(otherPolygon).getSpatialReference shouldBe srid
        polygon.rotate(45).getSpatialReference shouldBe srid
        polygon.scale(2d, 2d).getSpatialReference shouldBe srid
        polygon.simplify(0.001).getSpatialReference shouldBe srid
        polygon.translate(2d, 2d).getSpatialReference shouldBe srid
        polygon.union(otherPolygon).getSpatialReference shouldBe srid

        // MosaicPolygon
        polygon.flatten.head.getSpatialReference shouldBe srid
        polygon.getShellPoints.head.head.getSpatialReference shouldBe srid
        polygon.getHolePoints.head.head.head.getSpatialReference shouldBe srid

        // MosaicPolygonESRI
        polygon.asSeq.head.getSpatialReference shouldBe srid
        polygon.getBoundary.getSpatialReference shouldBe srid
        polygon.getHoles.head.head.getSpatialReference shouldBe srid
        polygon.getShells.head.getSpatialReference shouldBe srid
        polygon.mapXY({ (x: Double, y: Double) => (x * 2, y / 2) }).getSpatialReference shouldBe srid
    }

    "MosaicPolygonESRI" should "correctly apply CRS transformation" in {
        val sridSource = 4326
        val sridTarget = 27700
        val testPolygon = MosaicPolygonESRI
            .fromWKT(
              "POLYGON((-0.1367293 51.5166525, -0.1370977 51.517082, -0.1380077 51.5186537, -0.1375356 51.518824, -0.1371474 51.5184174, -0.1361386 51.5167553, -0.1367293 51.5166525))"
            )
            .asInstanceOf[MosaicPolygonESRI]
        testPolygon.setSpatialReference(sridSource)
        val expectedResult = MosaicPolygonESRI
            .fromWKT(
              "POLYGON((529382.90 181393.19, 529356.12 181440.30, 529288.54 181613.47, 529320.81 181633.24, 529348.89 181588.71, 529423.59 181405.67, 529382.90 181393.19))"
            )
            .asInstanceOf[MosaicPolygonESRI]
        val testResult = testPolygon.transformCRSXY(sridTarget).asInstanceOf[MosaicPolygonESRI]
        val intersection = expectedResult.intersection(testResult)
        intersection.getArea shouldBe expectedResult.getArea +- 1d
    }

}
