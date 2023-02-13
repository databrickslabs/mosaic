package com.databricks.labs.mosaic.core.geometry.multipolygon

import com.databricks.labs.mosaic.core.geometry.MosaicGeometryJTS
import com.databricks.labs.mosaic.core.geometry.polygon.MosaicPolygonJTS
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.apache.spark.sql.catalyst.InternalRow

class TestMultiPolygonJTS extends AnyFlatSpec {

    "MosaicMultiPolygonJTS" should "return Nil for holes and hole points calls." in {
        val multiPolygon = MosaicMultiPolygonJTS.fromWKT("MULTIPOLYGON(((0 1,3 0,4 3,0 4,0 1)), ((3 4,6 3,5 5,3 4)))")
        multiPolygon.getHoles should contain theSameElementsAs Seq(Nil, Nil)
        multiPolygon.getHolePoints should contain theSameElementsAs Seq(Nil, Nil)
    }

    "MosaicMultiPolygonJTS" should "return seq(this) for shells and flatten calls." in {
        val multiPolygon = MosaicMultiPolygonJTS.fromWKT("MULTIPOLYGON(((0 1,3 0,4 3,0 4,0 1)), ((3 4,6 3,5 5,3 4)))")
        val polygon = MosaicPolygonJTS.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")
        multiPolygon.getShells.head.equals(polygon.getShells.head) shouldBe true
        multiPolygon.flatten.head.equals(polygon) shouldBe true
    }

    "MosaicMultiPolygonJTS" should "return seq(this) for shells and flatten calls with holes." in {
        val multiPolygon = MosaicMultiPolygonJTS.fromWKT(
            "MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0), (1 1,9 1,9 9,1 9,1 1)), ((20 20,30 20,30 30,20 30,20 20), (21 21,29 21,29 29,21 29,21 21)))"
        )
        val polygon1 = MosaicPolygonJTS.fromWKT("POLYGON((0 0,10 0,10 10,0 10,0 0), (1 1,9 1,9 9,1 9,1 1))")
        val polygon2 = MosaicPolygonJTS.fromWKT("POLYGON((20 20,30 20,30 30,20 30,20 20), (21 21,29 21,29 29,21 29,21 21))")

        multiPolygon.flatten.head.equals(polygon1) shouldBe true
        multiPolygon.flatten.last.equals(polygon2) shouldBe true
    }

    "MosaicMultiPolygonJTS" should "return seq(this) for flatten calls with holes for internal geometries" in {
        val internalGeom = MosaicMultiPolygonJTS.fromWKT(
            "MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0), (1 1,9 1,9 9,1 9,1 1)), ((20 20,30 20,30 30,20 30,20 20), (21 21,29 21,29 29,21 29,21 21)))"
        ).toInternal

        val geom = MosaicMultiPolygonJTS.fromInternal(internalGeom.serialize.asInstanceOf[InternalRow])

        val polygon1 = MosaicPolygonJTS.fromWKT("POLYGON((0 0,10 0,10 10,0 10,0 0), (1 1,9 1,9 9,1 9,1 1))")
        val polygon2 = MosaicPolygonJTS.fromWKT("POLYGON((20 20,30 20,30 30,20 30,20 20), (21 21,29 21,29 29,21 29,21 21))")

        geom.flatten.head.equals(polygon1) shouldBe true
        geom.flatten.last.equals(polygon2) shouldBe true
    }

    "MosaicMultiPolygonJTS" should "flatten cells with holes." in {
        val json = "{\"type\": \"MultiPolygon\", \"coordinates\": [[[[5, 5], [0, 0], [10, 0], [5, 5]]], [[[100, 200], [100, 100], [200, 100], [200, 200], [100, 200]], [[175, 125], [125, 125], [125, 175], [175, 175], [175, 125]]], [[[25, 25], [20, 20], [30, 20], [25, 25]]]]}"
        val geom = MosaicGeometryJTS.fromJSON(json)

        val expected1 = MosaicGeometryJTS.fromWKT("POLYGON ((100 200, 100 100, 200 100, 200 200, 100 200), (175 125, 125 125, 125 175, 175 175, 175 125))")

        geom.flatten(1).equals(expected1) shouldBe true
    }

    "MosaicMultiPolygonJTS" should "return number of points." in {
        val multiPolygon = MosaicMultiPolygonJTS.fromWKT("MULTIPOLYGON(((0 1,3 0,4 3,0 4,0 1)), ((3 4,6 3,5 5,3 4)))")
        multiPolygon.numPoints shouldEqual 9
    }

    "MosaicMultiPolygonJTS" should "read all supported formats" in {
        val multiPolygon = MosaicMultiPolygonJTS.fromWKT("MULTIPOLYGON(((0 1,3 0,4 3,0 4,0 1)), ((3 4,6 3,5 5,3 4)))")
        noException should be thrownBy MosaicMultiPolygonJTS.fromWKB(multiPolygon.toWKB)
        noException should be thrownBy MosaicMultiPolygonJTS.fromHEX(multiPolygon.toHEX)
        noException should be thrownBy MosaicMultiPolygonJTS.fromJSON(multiPolygon.toJSON)
        noException should be thrownBy MosaicMultiPolygonJTS.fromInternal(multiPolygon.toInternal.serialize.asInstanceOf[InternalRow])
        multiPolygon.equals(MosaicMultiPolygonJTS.fromWKB(multiPolygon.toWKB)) shouldBe true
        multiPolygon.equals(MosaicMultiPolygonJTS.fromHEX(multiPolygon.toHEX)) shouldBe true
        multiPolygon.equals(MosaicMultiPolygonJTS.fromJSON(multiPolygon.toJSON)) shouldBe true
        multiPolygon.equals(MosaicMultiPolygonJTS.fromInternal(multiPolygon.toInternal.serialize.asInstanceOf[InternalRow])) shouldBe true
    }

    "MosaicMultiPolygonJTS" should "be instantiable from a Seq of MosaicPolygonJTS" in {
        val multiPolygonReference = MosaicMultiPolygonJTS.fromWKT(
          "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))"
        )
        val polygonSeq = Seq(
          "POLYGON ((40 40, 20 45, 45 30, 40 40))",
          "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20))"
        )
            .map(MosaicPolygonJTS.fromWKT)
            .map(_.asInstanceOf[MosaicPolygonJTS])
        val multiPolygonTest = MosaicMultiPolygonJTS.fromSeq(polygonSeq)
        multiPolygonReference.equals(multiPolygonTest) shouldBe true
    }

    "MosaicMultiPolygonJTS" should "not fail for empty Seq" in {
        val expected = MosaicMultiPolygonJTS.fromWKT(
            "MULTIPOLYGON EMPTY"
        )
        val actual = MosaicMultiPolygonJTS.fromSeq(Seq[MosaicPolygonJTS]())
        expected.equals(actual) shouldBe true
    }

    "MosaicMultiPolygonJTS" should "return a Seq of MosaicPolygonJTS object when calling asSeq" in {
        val multiPolygon = MosaicMultiPolygonJTS
            .fromWKT(
              "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))"
            )
            .asInstanceOf[MosaicMultiPolygonJTS]
        val polygonSeqReference = Seq(
          "POLYGON ((40 40, 20 45, 45 30, 40 40))",
          "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20))"
        )
            .map(MosaicPolygonJTS.fromWKT)
            .map(_.asInstanceOf[MosaicPolygonJTS])
        val polygonSeqTest = multiPolygon.asSeq.map(_.asInstanceOf[MosaicPolygonJTS])
        val results = polygonSeqReference
            .zip(polygonSeqTest)
            .map { case (a: MosaicPolygonJTS, b: MosaicPolygonJTS) => a.equals(b) }
        results should contain only true
    }

    "MosaicMultiPolygonJTS" should "return a Seq of MosaicPolygonJTS objects with the correct SRID when calling asSeq" in {
        val srid = 32632
        val multiPolygon = MosaicMultiPolygonJTS
            .fromWKT(
              "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))"
            )
            .asInstanceOf[MosaicMultiPolygonJTS]
        multiPolygon.setSpatialReference(srid)
        val polygonSeqReference = Seq(
          "POLYGON ((40 40, 20 45, 45 30, 40 40))",
          "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20))"
        )
            .map(MosaicPolygonJTS.fromWKT)
            .map(_.asInstanceOf[MosaicPolygonJTS])
        polygonSeqReference.foreach(_.setSpatialReference(srid))
        val polygonSeqTest = multiPolygon.asSeq.map(_.asInstanceOf[MosaicPolygonJTS])
        polygonSeqTest.map(_.getSpatialReference) should contain only srid

        val results = polygonSeqReference
            .zip(polygonSeqTest)
            .map { case (a: MosaicPolygonJTS, b: MosaicPolygonJTS) => a.getSpatialReference == b.getSpatialReference }
        results should contain only true
    }

    "MosaicPolygonJTS" should "maintain SRID across operations" in {
        val srid = 32632
        val multiPolygon = MosaicMultiPolygonJTS
            .fromWKT(
              "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))"
            )
            .asInstanceOf[MosaicMultiPolygonJTS]
        val otherPolygon = MosaicPolygonJTS.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")

        multiPolygon.setSpatialReference(srid)

        // MosaicGeometryJTS
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

        // MosaicMultiPolygonJTS
        multiPolygon.asSeq.head.getSpatialReference shouldBe srid
        multiPolygon.getBoundary.getSpatialReference shouldBe srid
        multiPolygon.getHoles.last.head.getSpatialReference shouldBe srid
        multiPolygon.getShells.head.getSpatialReference shouldBe srid
        multiPolygon.mapXY({ (x: Double, y: Double) => (x * 2, y / 2) }).getSpatialReference shouldBe srid
    }

}
