package com.databricks.labs.mosaic.core.geometry.multipoint

import com.databricks.labs.mosaic.core.geometry.point.MosaicPointJTS
import com.databricks.labs.mosaic.core.geometry.polygon.MosaicPolygonJTS
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.catalyst.InternalRow

//noinspection ScalaRedundantCast
class TestMultiPointJTS extends AnyFlatSpec {

    "MosaicMultiPointJTS" should "return Nil for holes and hole points calls." in {
        val multiPoint = MosaicMultiPointJTS.fromWKT("MULTIPOINT (1 1, 2 2, 3 3)")
        multiPoint.getHoles shouldEqual Nil
        multiPoint.getHolePoints shouldEqual Nil
    }

    "MosaicMultiPointJTS" should "return seq(this) for shells and flatten calls." in {
        val multiPoint = MosaicMultiPointJTS.fromWKT("MULTIPOINT (1 1, 2 2, 3 3)")
        val point = MosaicPointJTS.fromWKT("POINT (1 1)")
        the[Exception] thrownBy
            multiPoint.getShells.head.equals(point) should have
        message("getShells should not be called on MultiPoints.")
        multiPoint.flatten.head.equals(point) shouldBe true
    }

    "MosaicMultiPointJTS" should "return number of points." in {
        val multiPoint = MosaicMultiPointJTS.fromWKT("MULTIPOINT (1 1, 2 2, 3 3)")
        multiPoint.numPoints shouldEqual 3
    }

    "MosaicMultiPointJTS" should "read all supported formats" in {
        val multiPoint = MosaicMultiPointJTS.fromWKT("MULTIPOINT (1 1, 2 2, 3 3)")
        noException should be thrownBy MosaicMultiPointJTS.fromWKB(multiPoint.toWKB)
        noException should be thrownBy MosaicMultiPointJTS.fromHEX(multiPoint.toHEX)
        noException should be thrownBy MosaicMultiPointJTS.fromJSON(multiPoint.toJSON)
        noException should be thrownBy MosaicMultiPointJTS.fromInternal(multiPoint.toInternal.serialize.asInstanceOf[InternalRow])
        multiPoint.equals(MosaicMultiPointJTS.fromWKB(multiPoint.toWKB)) shouldBe true
        multiPoint.equals(MosaicMultiPointJTS.fromHEX(multiPoint.toHEX)) shouldBe true
        multiPoint.equals(MosaicMultiPointJTS.fromJSON(multiPoint.toJSON)) shouldBe true
        multiPoint.equals(MosaicMultiPointJTS.fromInternal(multiPoint.toInternal.serialize.asInstanceOf[InternalRow])) shouldBe true
    }

    "MosaicMultiPointJTS" should "be instantiable from a Seq of MosaicPointJTS" in {
        val multiPointReference = MosaicMultiPointJTS.fromWKT("MULTIPOINT (1 1, 2 2, 3 3)")
        val pointsSeq = Seq("POINT (1 1)", "POINT (2 2)", "POINT (3 3)")
            .map(MosaicPointJTS.fromWKT)
            .map(_.asInstanceOf[MosaicPointJTS])
        val multiPointTest = MosaicMultiPointJTS.fromSeq(pointsSeq)
        multiPointReference.equals(multiPointTest) shouldBe true
    }

    "MosaicMultiPointJTS" should "return a Seq of MosaicPointJTS object when calling asSeq" in {
        val multiPoint = MosaicMultiPointJTS.fromWKT("MULTIPOINT (1 1, 2 2, 3 3)").asInstanceOf[MosaicMultiPointJTS]
        val pointsSeqReference = Seq("POINT (1 1)", "POINT (2 2)", "POINT (3 3)")
            .map(MosaicPointJTS.fromWKT)
            .map(_.asInstanceOf[MosaicPointJTS])
        val pointSeqTest = multiPoint.asSeq.map(_.asInstanceOf[MosaicPointJTS])
        val results = pointsSeqReference
            .zip(pointSeqTest)
            .map { case (a: MosaicPointJTS, b: MosaicPointJTS) => a.equals(b) }
        results should contain only true
    }

    "MosaicMultiPointJTS" should "not fail for empty Seq" in {
        val expected = MosaicMultiPointJTS.fromWKT(
            "MULTIPOINT EMPTY"
        )
        val actual = MosaicMultiPointJTS.fromSeq(Seq[MosaicMultiPointJTS]())
        expected.equals(actual) shouldBe true
    }


    "MosaicMultiPointJTS" should "return a Seq of MosaicPointJTS object with the correct SRID when calling asSeq" in {
        val srid = 32632
        val multiPoint = MosaicMultiPointJTS
            .fromWKT("MULTIPOINT (1 1, 2 2, 3 3)")
            .asInstanceOf[MosaicMultiPointJTS]
        multiPoint.setSpatialReference(srid)
        val pointsSeqReference = Seq("POINT (1 1)", "POINT (2 2)", "POINT (3 3)")
            .map(MosaicPointJTS.fromWKT)
            .map(_.asInstanceOf[MosaicPointJTS])
        pointsSeqReference.foreach(_.setSpatialReference(srid))

        val pointSeqTest = multiPoint.asSeq.map(_.asInstanceOf[MosaicPointJTS])
        pointSeqTest.map(_.getSpatialReference) should contain only srid

        val results = pointsSeqReference
            .zip(pointSeqTest)
            .map { case (a: MosaicPointJTS, b: MosaicPointJTS) => a.getSpatialReference == b.getSpatialReference }
        results should contain only true
    }

    "MosaicMultiPointJTS" should "maintain SRID across operations" in {
        val srid = 32632
        val multiPoint = MosaicMultiPointJTS.fromWKT("MULTIPOINT (1 1, 2 2, 3 3)").asInstanceOf[MosaicMultiPointJTS]
        val anotherPoint = MosaicPointJTS.fromWKT("POINT(1 1)").asInstanceOf[MosaicPointJTS]
        val poly = MosaicPolygonJTS.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")

        multiPoint.setSpatialReference(srid)

        // MosaicGeometryJTS
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

        // MosaicMultiPointJTS
        multiPoint.getBoundary.getSpatialReference shouldBe srid
        multiPoint.mapXY({ (x: Double, y: Double) => (x * 2, y / 2) }).getSpatialReference shouldBe srid
    }

}
