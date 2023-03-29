package com.databricks.labs.mosaic.core.geometry.multipoint

import com.databricks.labs.mosaic.core.geometry.point.MosaicPointESRI
import com.databricks.labs.mosaic.core.geometry.polygon.MosaicPolygonESRI
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.catalyst.InternalRow

//noinspection ScalaRedundantCast
class TestMultiPointESRI extends AnyFlatSpec {

    "MosaicMultiPointESRI" should "return Nil for holes and hole points calls." in {
        val multiPoint = MosaicMultiPointESRI.fromWKT("MULTIPOINT (1 1, 2 2, 3 3)")
        multiPoint.getHoles shouldEqual Nil
        multiPoint.getHolePoints shouldEqual Nil
    }

    "MosaicMultiPointESRI" should "return seq(this) for shells and flatten calls." in {
        val multiPoint = MosaicMultiPointESRI.fromWKT("MULTIPOINT (1 1, 2 2, 3 3)")
        val point = MosaicPointESRI.fromWKT("POINT (1 1)")
        the[Exception] thrownBy
            multiPoint.getShells.head.equals(point) should have
        message("getShells should not be called on MultiPoints.")
        multiPoint.flatten.head.equals(point) shouldBe true
    }

    "MosaicMultiPointESRI" should "return number of points." in {
        val multiPoint = MosaicMultiPointESRI.fromWKT("MULTIPOINT (1 1, 2 2, 3 3)")
        multiPoint.numPoints shouldEqual 3
    }

    "MosaicMultiPointESRI" should "read all supported formats" in {
        val multiPoint = MosaicMultiPointESRI.fromWKT("MULTIPOINT (1 1, 2 2, 3 3)")
        noException should be thrownBy MosaicMultiPointESRI.fromWKB(multiPoint.toWKB)
        noException should be thrownBy MosaicMultiPointESRI.fromHEX(multiPoint.toHEX)
        noException should be thrownBy MosaicMultiPointESRI.fromJSON(multiPoint.toJSON)
        noException should be thrownBy MosaicMultiPointESRI.fromInternal(multiPoint.toInternal.serialize.asInstanceOf[InternalRow])
        multiPoint.equals(MosaicMultiPointESRI.fromWKB(multiPoint.toWKB)) shouldBe true
        multiPoint.equals(MosaicMultiPointESRI.fromHEX(multiPoint.toHEX)) shouldBe true
        multiPoint.equals(MosaicMultiPointESRI.fromJSON(multiPoint.toJSON)) shouldBe true
        multiPoint.equals(MosaicMultiPointESRI.fromInternal(multiPoint.toInternal.serialize.asInstanceOf[InternalRow])) shouldBe true
    }

    "MosaicMultiPointESRI" should "be instantiable from a Seq of MosaicPointESRI" in {
        val multiPointReference = MosaicMultiPointESRI.fromWKT("MULTIPOINT (1 1, 2 2, 3 3)")
        val pointsSeq = Seq("POINT (1 1)", "POINT (2 2)", "POINT (3 3)")
            .map(MosaicPointESRI.fromWKT)
            .map(_.asInstanceOf[MosaicPointESRI])
        val multiPointTest = MosaicMultiPointESRI.fromSeq(pointsSeq)
        multiPointReference.equals(multiPointTest) shouldBe true
    }

    "MosaicMultiPointESRI" should "not fail for empty Seq" in {
        val expected = MosaicMultiPointESRI.fromWKT(
            "MULTIPOINT EMPTY"
        )
        val actual = MosaicMultiPointESRI.fromSeq(Seq())
        expected.equals(actual) shouldBe true
    }

    "MosaicMultiPointESRI" should "return a Seq of MosaicPointESRI object when calling asSeq" in {
        val multiPoint = MosaicMultiPointESRI.fromWKT("MULTIPOINT (1 1, 2 2, 3 3)").asInstanceOf[MosaicMultiPointESRI]
        val pointsSeqReference = Seq("POINT (1 1)", "POINT (2 2)", "POINT (3 3)")
            .map(MosaicPointESRI.fromWKT)
            .map(_.asInstanceOf[MosaicPointESRI])
        val pointSeqTest = multiPoint.asSeq.map(_.asInstanceOf[MosaicPointESRI])
        val results = pointsSeqReference
            .zip(pointSeqTest)
            .map { case (a: MosaicPointESRI, b: MosaicPointESRI) => a.equals(b) }
        results should contain only true
    }

    "MosaicMultiPointESRI" should "return a Seq of MosaicPointESRI object with the correct SRID when calling asSeq" in {
        val srid = 32632
        val multiPoint = MosaicMultiPointESRI
            .fromWKT("MULTIPOINT (1 1, 2 2, 3 3)")
            .asInstanceOf[MosaicMultiPointESRI]
        multiPoint.setSpatialReference(srid)
        val pointsSeqReference = Seq("POINT (1 1)", "POINT (2 2)", "POINT (3 3)")
            .map(MosaicPointESRI.fromWKT)
            .map(_.asInstanceOf[MosaicPointESRI])
        pointsSeqReference.foreach(_.setSpatialReference(srid))
        val pointSeqTest = multiPoint.asSeq.map(_.asInstanceOf[MosaicPointESRI])

        pointSeqTest.map(_.getSpatialReference) should contain only srid

        val results = pointsSeqReference
            .zip(pointSeqTest)
            .map { case (a: MosaicPointESRI, b: MosaicPointESRI) => a.getSpatialReference == b.getSpatialReference }
        results should contain only true
    }

    "MosaicMultiPointESRI" should "maintain SRID across operations" in {
        val srid = 32632
        val multiPoint = MosaicMultiPointESRI.fromWKT("MULTIPOINT (1 1, 2 2, 3 3)").asInstanceOf[MosaicMultiPointESRI]
        val anotherPoint = MosaicPointESRI.fromWKT("POINT(1 1)").asInstanceOf[MosaicPointESRI]
        val poly = MosaicPolygonESRI.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")

        multiPoint.setSpatialReference(srid)

        // MosaicGeometryESRI
        multiPoint.buffer(2d).getSpatialReference shouldBe srid
        multiPoint.convexHull.getSpatialReference shouldBe srid
        multiPoint.getCentroid.getSpatialReference shouldBe srid
        multiPoint.intersection(poly).getSpatialReference shouldBe srid
        multiPoint.rotate(45).getSpatialReference shouldBe srid
        multiPoint.scale(2d, 2d).getSpatialReference shouldBe srid
        multiPoint.simplify(0.001).getSpatialReference shouldBe srid
        multiPoint.translate(2d, 2d).getSpatialReference shouldBe srid
        multiPoint.union(anotherPoint).getSpatialReference shouldBe srid

        // MosaicMultiPoint
        multiPoint.asSeq.head.getSpatialReference shouldBe srid
        multiPoint.flatten.head.getSpatialReference shouldBe srid
        multiPoint.getShellPoints.head.head.getSpatialReference shouldBe srid

        // MosaicMultiPointESRI
        multiPoint.getBoundary.getSpatialReference shouldBe srid
        multiPoint.mapXY({ (x: Double, y: Double) => (x * 2, y / 2) }).getSpatialReference shouldBe srid
    }

}
