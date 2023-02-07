package com.databricks.labs.mosaic.core.geometry.multipolygon

import com.databricks.labs.mosaic.core.geometry.polygon.MosaicPolygonESRI
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.catalyst.InternalRow

class TestMultiPolygonESRI extends AnyFlatSpec {

    "MosaicMultiPolygonESRI" should "return Nil for holes and hole points calls." in {
        val multiPolygon = MosaicMultiPolygonESRI.fromWKT("MULTIPOLYGON(((0 1,3 0,4 3,0 4,0 1)), ((3 4,6 3,5 5,3 4)))")
        multiPolygon.getHoles should contain theSameElementsAs Seq(Nil, Nil)
        multiPolygon.getHolePoints should contain theSameElementsAs Seq(Nil, Nil)
    }

    "MosaicMultiPolygonESRI" should "return seq(this) for shells and flatten calls." in {
        val multiPolygon = MosaicMultiPolygonESRI.fromWKT("MULTIPOLYGON(((0 1,3 0,4 3,0 4,0 1)), ((3 4,6 3,5 5,3 4)))")
        val polygon = MosaicPolygonESRI.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")
        multiPolygon.getShells.head.equals(polygon.getShells.head) shouldBe true
        multiPolygon.flatten.head.equals(polygon) shouldBe true
    }

    "MosaicMultiPolygonESRI" should "return number of points." in {
        val multiPolygon = MosaicMultiPolygonESRI.fromWKT("MULTIPOLYGON(((0 1,3 0,4 3,0 4,0 1)), ((3 4,6 3,5 5,3 4)))")
        multiPolygon.numPoints shouldEqual 9
    }

    "MosaicMultiPolygonESRI" should "read all supported formats" in {
        val multiPolygon = MosaicMultiPolygonESRI.fromWKT("MULTIPOLYGON(((0 1,3 0,4 3,0 4,0 1)), ((3 4,6 3,5 5,3 4)))")
        noException should be thrownBy MosaicMultiPolygonESRI.fromWKB(multiPolygon.toWKB)
        noException should be thrownBy MosaicMultiPolygonESRI.fromHEX(multiPolygon.toHEX)
        noException should be thrownBy MosaicMultiPolygonESRI.fromJSON(multiPolygon.toJSON)
        noException should be thrownBy MosaicMultiPolygonESRI.fromInternal(multiPolygon.toInternal.serialize.asInstanceOf[InternalRow])
        multiPolygon.equals(MosaicMultiPolygonESRI.fromWKB(multiPolygon.toWKB)) shouldBe true
        multiPolygon.equals(MosaicMultiPolygonESRI.fromHEX(multiPolygon.toHEX)) shouldBe true
        multiPolygon.equals(MosaicMultiPolygonESRI.fromJSON(multiPolygon.toJSON)) shouldBe true
        multiPolygon.equals(MosaicMultiPolygonESRI.fromInternal(multiPolygon.toInternal.serialize.asInstanceOf[InternalRow])) shouldBe true
    }

    "MosaicMultiPolygonESRI" should "be instantiable from a Seq of MosaicPolygonESRI" in {
        val multiPolygonReference = MosaicMultiPolygonESRI.fromWKT(
          "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))"
        )
        val polygonSeq = Seq(
          "POLYGON ((40 40, 20 45, 45 30, 40 40))",
          "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20))"
        )
            .map(MosaicPolygonESRI.fromWKT)
            .map(_.asInstanceOf[MosaicPolygonESRI])
        val multiPolygonTest = MosaicMultiPolygonESRI.fromSeq(polygonSeq)
        multiPolygonReference.equals(multiPolygonTest) shouldBe true
    }

    "MosaicMultiPolygonESRI" should "not fail for empty multipolygon from a Seq of MosaicPolygonESRI" in {
        val multiPolygonReference = MosaicMultiPolygonESRI.fromWKT(
            "MULTIPOLYGON EMPTY"
        )
        val multiPolygonTest = MosaicMultiPolygonESRI.fromSeq(Seq[MosaicPolygonESRI]())
        multiPolygonReference.equals(multiPolygonTest) shouldBe true
    }

    "MosaicMultiPolygonESRI" should "return a Seq of MosaicPolygonESRI object when calling asSeq" in {
        val multiPolygon = MosaicMultiPolygonESRI
            .fromWKT(
              "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))"
            )
            .asInstanceOf[MosaicMultiPolygonESRI]
        val polygonSeqReference = Seq(
          "POLYGON ((40 40, 20 45, 45 30, 40 40))",
          "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20))"
        )
            .map(MosaicPolygonESRI.fromWKT)
            .map(_.asInstanceOf[MosaicPolygonESRI])
        val polygonSeqTest = multiPolygon.asSeq.map(_.asInstanceOf[MosaicPolygonESRI])
        val results = polygonSeqReference
            .zip(polygonSeqTest)
            .map { case (a: MosaicPolygonESRI, b: MosaicPolygonESRI) => a.equals(b) }
        results should contain only true
    }

    "MosaicMultiPolygonESRI" should "return a Seq of MosaicPolygonESRI objects with the correct SRID when calling asSeq" in {
        val srid = 32632
        val multiPolygon = MosaicMultiPolygonESRI
            .fromWKT(
              "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))"
            )
            .asInstanceOf[MosaicMultiPolygonESRI]
        multiPolygon.setSpatialReference(srid)
        val polygonSeqReference = Seq(
          "POLYGON ((40 40, 20 45, 45 30, 40 40))",
          "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20))"
        )
            .map(MosaicPolygonESRI.fromWKT)
            .map(_.asInstanceOf[MosaicPolygonESRI])
        polygonSeqReference.foreach(_.setSpatialReference(srid))
        val polygonSeqTest = multiPolygon.asSeq.map(_.asInstanceOf[MosaicPolygonESRI])
        polygonSeqTest.map(_.getSpatialReference) should contain only srid

        val results = polygonSeqReference
            .zip(polygonSeqTest)
            .map { case (a: MosaicPolygonESRI, b: MosaicPolygonESRI) => a.getSpatialReference == b.getSpatialReference }
        results should contain only true
    }

    "MosaicPolygonESRI" should "maintain SRID across operations" in {
        val srid = 32632
        val multiPolygon = MosaicMultiPolygonESRI
            .fromWKT(
              "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))"
            )
            .asInstanceOf[MosaicMultiPolygonESRI]
        val otherPolygon = MosaicPolygonESRI.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")

        multiPolygon.setSpatialReference(srid)

        // MosaicGeometryESRI
        multiPolygon.buffer(2d).getSpatialReference shouldBe srid
        multiPolygon.convexHull.getSpatialReference shouldBe srid
        multiPolygon.getCentroid.getSpatialReference shouldBe srid
        multiPolygon.intersection(otherPolygon).getSpatialReference shouldBe srid
        multiPolygon.reduceFromMulti.getSpatialReference shouldBe srid
        multiPolygon.rotate(45).getSpatialReference shouldBe srid
        multiPolygon.scale(2d, 2d).getSpatialReference shouldBe srid
        multiPolygon.simplify(0.001).getSpatialReference shouldBe srid
        multiPolygon.translate(2d, 2d).getSpatialReference shouldBe srid
        multiPolygon.union(otherPolygon).getSpatialReference shouldBe srid

        // MosaicMultiPolygon
        multiPolygon.flatten.head.getSpatialReference shouldBe srid
        multiPolygon.getShellPoints.head.head.getSpatialReference shouldBe srid
        multiPolygon.getHolePoints.last.head.head.getSpatialReference shouldBe srid

        // MosaicMultiPolygonESRI
        multiPolygon.asSeq.head.getSpatialReference shouldBe srid
        multiPolygon.getBoundary.getSpatialReference shouldBe srid
        multiPolygon.getHoles.last.head.getSpatialReference shouldBe srid
        multiPolygon.getShells.head.getSpatialReference shouldBe srid
        multiPolygon.mapXY({ (x: Double, y: Double) => (x * 2, y / 2) }).getSpatialReference shouldBe srid
    }

}
