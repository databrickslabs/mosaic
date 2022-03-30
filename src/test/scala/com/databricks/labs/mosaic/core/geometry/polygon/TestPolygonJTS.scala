package com.databricks.labs.mosaic.core.geometry.polygon

import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineStringJTS
import com.databricks.labs.mosaic.core.geometry.point.MosaicPointJTS
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.catalyst.InternalRow

class TestPolygonJTS extends AnyFlatSpec {

    "MosaicPolygonJTS" should "return Nil for holes and hole points calls." in {
        val point = MosaicPolygonJTS.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")
        point.getHoles shouldEqual Seq(Nil)
        point.getHolePoints shouldEqual Seq(Nil)
    }

    "MosaicPolygonJTS" should "return seq(this) for shells and flatten calls." in {
        val polygon = MosaicPolygonJTS.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")
        val lineString = MosaicLineStringJTS.fromWKT("LINESTRING (0 1,3 0,4 3,0 4,0 1)")
        polygon.getShells.head.equals(lineString) shouldBe true
        polygon.flatten should contain theSameElementsAs Seq(polygon)
    }

    "MosaicPolygonJTS" should "return number of points." in {
        val polygon = MosaicPolygonJTS.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")
        polygon.numPoints shouldEqual 5
    }

    "MosaicPolygonJTS" should "read all supported formats" in {
        val polygon = MosaicPolygonJTS.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")
        noException should be thrownBy MosaicPolygonJTS.fromWKB(polygon.toWKB)
        noException should be thrownBy MosaicPolygonJTS.fromHEX(polygon.toHEX)
        noException should be thrownBy MosaicPolygonJTS.fromJSON(polygon.toJSON)
        noException should be thrownBy MosaicPolygonJTS.fromInternal(polygon.toInternal.serialize.asInstanceOf[InternalRow])
        polygon.equals(MosaicPolygonJTS.fromWKB(polygon.toWKB)) shouldBe true
        polygon.equals(MosaicPolygonJTS.fromHEX(polygon.toHEX)) shouldBe true
        polygon.equals(MosaicPolygonJTS.fromJSON(polygon.toJSON)) shouldBe true
        polygon.equals(MosaicPolygonJTS.fromInternal(polygon.toInternal.serialize.asInstanceOf[InternalRow])) shouldBe true
    }

    "MosaicPolygonJTS" should "be instantiable from a Seq of MosaicPointJTS" in {
        val polygonReference = MosaicPolygonJTS.fromWKT("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")
        val pointSeq = Seq("POINT (30 10)", "POINT (40 40)", "POINT (20 40)", "POINT (10 20)")
            .map(MosaicPointJTS.fromWKT)
            .map(_.asInstanceOf[MosaicPointJTS])
        val polygonTest = MosaicPolygonJTS.fromPoints(pointSeq)
        polygonReference.equals(polygonTest) shouldBe true
    }

    "MosaicPolygonJTS" should "be instantiable from a Seq of MosaicLineStringJTS" in {
        val polygonReference = MosaicPolygonJTS.fromWKT("POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))")
        val linesSeq = Seq("LINESTRING (35 10, 45 45, 15 40, 10 20)", "LINESTRING (20 30, 35 35, 30 20)")
            .map(MosaicLineStringJTS.fromWKT)
            .map(_.asInstanceOf[MosaicLineStringJTS])
        val polygonTest = MosaicPolygonJTS.fromLines(linesSeq)
        polygonReference.equals(polygonTest) shouldBe true
    }

    "MosaicPolygonJTS" should "return a Seq of MosaicLineStringJTS object when calling asSeq" in {
        val polygon = MosaicPolygonJTS
            .fromWKT("POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))")
            .asInstanceOf[MosaicPolygonJTS]
        val linesSeqReference = Seq("LINESTRING (35 10, 45 45, 15 40, 10 20, 35 10)", "LINESTRING (20 30, 35 35, 30 20, 20 30)")
            .map(MosaicLineStringJTS.fromWKT)
            .map(_.asInstanceOf[MosaicLineStringJTS])
        val lineSeqTest = polygon.asSeq.map(_.asInstanceOf[MosaicLineStringJTS])
        val results = linesSeqReference
            .zip(lineSeqTest)
            .map { case (a: MosaicLineStringJTS, b: MosaicLineStringJTS) => a.equals(b) }
        results should contain only true
    }

    "MosaicPolygonJTS" should "return a Seq of MosaicLineStringJTS object with the correct SRID when calling asSeq" in {
        val srid = 32632
        val polygon = MosaicPolygonJTS
            .fromWKT("POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))")
            .asInstanceOf[MosaicPolygonJTS]
        polygon.setSpatialReference(srid)
        val linesSeqReference = Seq("LINESTRING (35 10, 45 45, 15 40, 10 20, 35 10)", "LINESTRING (20 30, 35 35, 30 20, 20 30)")
            .map(MosaicLineStringJTS.fromWKT)
            .map(_.asInstanceOf[MosaicLineStringJTS])
        linesSeqReference.foreach(_.setSpatialReference(srid))
        val lineSeqTest = polygon.asSeq.map(_.asInstanceOf[MosaicLineStringJTS])
        lineSeqTest.map(_.getSpatialReference) should contain only srid

        val results = linesSeqReference
            .zip(lineSeqTest)
            .map { case (a: MosaicLineStringJTS, b: MosaicLineStringJTS) => a.getSpatialReference == b.getSpatialReference }
        results should contain only true
    }

    "MosaicPolygonJTS" should "maintain SRID across operations" in {
        val srid = 32632
        val polygon = MosaicPolygonJTS
            .fromWKT("POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))")
            .asInstanceOf[MosaicPolygonJTS]
        val otherPolygon = MosaicPolygonJTS.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")

        polygon.setSpatialReference(srid)

        // MosaicGeometryJTS
        polygon.buffer(2d).getSpatialReference shouldBe srid
        polygon.convexHull.getSpatialReference shouldBe srid
        polygon.getCentroid.getSpatialReference shouldBe srid
        polygon.intersection(otherPolygon).getSpatialReference shouldBe srid
        polygon.reduceFromMulti.getSpatialReference shouldBe srid
        polygon.rotate(45).getSpatialReference shouldBe srid
        polygon.scale(2d, 2d).getSpatialReference shouldBe srid
        polygon.simplify(0.001).getSpatialReference shouldBe srid
        polygon.translate(2d, 2d).getSpatialReference shouldBe srid
        polygon.union(otherPolygon).getSpatialReference shouldBe srid

        // MosaicPolygon
        polygon.flatten.head.getSpatialReference shouldBe srid
        polygon.getShellPoints.head.head.getSpatialReference shouldBe srid
        polygon.getHolePoints.head.head.head.getSpatialReference shouldBe srid

        // MosaicPolygonJTS
        polygon.asSeq.head.getSpatialReference shouldBe srid
        polygon.getBoundary.getSpatialReference shouldBe srid
        polygon.getHoles.head.head.getSpatialReference shouldBe srid
        polygon.getShells.head.getSpatialReference shouldBe srid
        polygon.mapXY({ (x: Double, y: Double) => (x * 2, y / 2) }).getSpatialReference shouldBe srid
    }

}
