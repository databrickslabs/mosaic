package com.databricks.labs.mosaic.core.geometry.multilinestring

import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineStringESRI
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.catalyst.InternalRow

class TestMultiLineStringESRI extends AnyFlatSpec {

    "MosaicMultiLineStringESRI" should "return Nil for holes and hole points calls." in {
        val multiLineString = MosaicMultiLineStringESRI.fromWKT("MULTILINESTRING ((1 1, 2 2, 3 3), (2 2, 3 3, 4 4))")
        multiLineString.getHoles shouldEqual Nil
        multiLineString.getHolePoints shouldEqual Nil
    }

    "MosaicMultiLineStringESRI" should "return seq(this) for shells and flatten calls." in {
        val multiLineString = MosaicMultiLineStringESRI.fromWKT("MULTILINESTRING ((1 1, 2 2, 3 3), (2 2, 3 3, 4 4))")
        val lineString = MosaicLineStringESRI.fromWKT("LINESTRING(1 1, 2 2, 3 3)")
        multiLineString.getShells.head.equals(lineString) shouldBe true
        multiLineString.flatten.head.equals(lineString) shouldBe true
    }

    "MosaicMultiLineStringESRI" should "return number of points." in {
        val multiLineString = MosaicMultiLineStringESRI.fromWKT("MULTILINESTRING ((1 1, 2 2, 3 3), (2 2, 3 3, 4 4))")
        multiLineString.numPoints shouldEqual 6
    }

    "MosaicMultiLineStringESRI" should "read all supported formats" in {
        val multiLineString = MosaicMultiLineStringESRI.fromWKT("MULTILINESTRING ((1 1, 2 2, 3 3), (2 2, 3 3, 4 4))")
        noException should be thrownBy MosaicMultiLineStringESRI.fromWKB(multiLineString.toWKB)
        noException should be thrownBy MosaicMultiLineStringESRI.fromHEX(multiLineString.toHEX)
        noException should be thrownBy MosaicMultiLineStringESRI.fromJSON(multiLineString.toJSON)
        noException should be thrownBy MosaicMultiLineStringESRI.fromInternal(
          multiLineString.toInternal.serialize.asInstanceOf[InternalRow]
        )
        multiLineString.equals(MosaicMultiLineStringESRI.fromWKB(multiLineString.toWKB)) shouldBe true
        multiLineString.equals(MosaicMultiLineStringESRI.fromHEX(multiLineString.toHEX)) shouldBe true
        multiLineString.equals(MosaicMultiLineStringESRI.fromJSON(multiLineString.toJSON)) shouldBe true
        multiLineString.equals(
          MosaicMultiLineStringESRI.fromInternal(multiLineString.toInternal.serialize.asInstanceOf[InternalRow])
        ) shouldBe true
    }

    "MosaicMultiLineStringESRI" should "be instantiable from a Seq of MosaicLineStringESRI" in {
        val lineStringReference = MosaicMultiLineStringESRI.fromWKT("MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))")
        val linesSeq = Seq("LINESTRING (10 10, 20 20, 10 40)", "LINESTRING (40 40, 30 30, 40 20, 30 10)")
            .map(MosaicLineStringESRI.fromWKT)
            .map(_.asInstanceOf[MosaicLineStringESRI])
        val lineStringTest = MosaicMultiLineStringESRI.fromLines(linesSeq)
        lineStringReference.equals(lineStringTest) shouldBe true
    }

//    "MosaicLineStringESRI" should "return a Seq of MosaicPointESRI object when calling asSeq" in {
//        val lineString = MosaicLineStringESRI.fromWKT("LINESTRING (1 1, 2 2, 3 3)").asInstanceOf[MosaicLineStringESRI]
//        val pointsSeqReference = Seq("POINT (1 1)", "POINT (2 2)", "POINT (3 3)")
//            .map(MosaicPointESRI.fromWKT)
//            .map(_.asInstanceOf[MosaicPointESRI])
//        val pointSeqTest = lineString.asSeq.map(_.asInstanceOf[MosaicPointESRI])
//        val results = pointsSeqReference
//            .zip(pointSeqTest)
//            .map { case (a: MosaicPointESRI, b: MosaicPointESRI) => a.equals(b) }
//        results should contain only true
//    }
//
//    "MosaicLineStringESRI" should "return a Seq of MosaicPointESRI object with the correct SRID when calling asSeq" in {
//        val srid = 32632
//        val lineString = MosaicLineStringESRI
//            .fromWKT("LINESTRING (1 1, 2 2, 3 3)")
//            .asInstanceOf[MosaicLineStringESRI]
//        lineString.setSpatialReference(srid)
//        val pointsSeqReference = Seq("POINT (1 1)", "POINT (2 2)", "POINT (3 3)")
//            .map(MosaicPointESRI.fromWKT)
//            .map(_.asInstanceOf[MosaicPointESRI])
//        pointsSeqReference.foreach(_.setSpatialReference(srid))
//        val pointSeqTest = lineString.asSeq.map(_.asInstanceOf[MosaicPointESRI])
//
//        pointSeqTest.map(_.getSpatialReference) should contain only srid
//
//        val results = pointsSeqReference
//            .zip(pointSeqTest)
//            .map { case (a: MosaicPointESRI, b: MosaicPointESRI) => a.getSpatialReference == b.getSpatialReference }
//        results should contain only true
//    }
//
//    "MosaicLineStringESRI" should "maintain SRID across operations" in {
//        val srid = 32632
//        val lineString = MosaicLineStringESRI.fromWKT("LINESTRING (1 1, 2 2, 3 3)").asInstanceOf[MosaicLineStringESRI]
//        val anotherPoint = MosaicPointESRI.fromWKT("POINT(1 1)").asInstanceOf[MosaicPointESRI]
//        val poly = MosaicPolygonESRI.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")
//
//        lineString.setSpatialReference(srid)
//
//        // MosaicGeometryESRI
//        lineString.buffer(2d).getSpatialReference shouldBe srid
//        lineString.convexHull.getSpatialReference shouldBe srid
//        lineString.getCentroid.getSpatialReference shouldBe srid
//        lineString.intersection(poly).getSpatialReference shouldBe srid
//        lineString.reduceFromMulti.getSpatialReference shouldBe srid
//        lineString.rotate(45).getSpatialReference shouldBe srid
//        lineString.scale(2d, 2d).getSpatialReference shouldBe srid
//        lineString.simplify(0.001).getSpatialReference shouldBe srid
//        lineString.translate(2d, 2d).getSpatialReference shouldBe srid
//        lineString.union(anotherPoint).getSpatialReference shouldBe srid
//
//        // MosaicLineString
//        lineString.asSeq.head.getSpatialReference shouldBe srid
//        lineString.flatten.head.getSpatialReference shouldBe srid
//        lineString.getShells.head.getSpatialReference shouldBe srid
//
//        // MosaicLineStringESRI
//        lineString.getBoundary.getSpatialReference shouldBe srid
//        lineString.getShellPoints.head.head.getSpatialReference shouldBe srid
//        lineString.mapXY({ (x: Double, y: Double) => (x * 2, y / 2) }).getSpatialReference shouldBe srid
//    }

}
