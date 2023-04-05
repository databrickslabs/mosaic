package com.databricks.labs.mosaic.core.geometry.point

import com.databricks.labs.mosaic.core.geometry.polygon.MosaicPolygonJTS
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.catalyst.InternalRow

class TestPointJTS extends AnyFlatSpec {

    "MosaicPointJTS" should "return Nil for holes and hole points calls." in {
        val point = MosaicPointJTS.fromWKT("POINT(1 1)")
        point.getHoles shouldEqual Nil
        point.getHolePoints shouldEqual Nil
    }

    "MosaicPointJTS" should "return seq(this) for shells and flatten calls." in {
        val point = MosaicPointJTS.fromWKT("POINT(1 1)")
        the[Exception] thrownBy
            point.getShells should have
        message("getShells should not be called on MultiPoints.")
        point.flatten should contain theSameElementsAs Seq(point)
    }

    "MosaicPointJTS" should "return number of points." in {
        val point = MosaicPointJTS.fromWKT("POINT(1 1)")
        point.numPoints shouldEqual 1
    }

    "MosaicPointJTS" should "be instantiable from a Seq of MosaicPointJTS" in {
        val lineStringReference = MosaicPointJTS.fromWKT("POINT (1 1)")
        val pointsSeq = Seq("POINT (1 1)")
          .map(MosaicPointJTS.fromWKT)
          .map(_.asInstanceOf[MosaicPointJTS])
        val lineStringTest = MosaicPointJTS.fromSeq(pointsSeq)
        lineStringReference.equals(lineStringTest) shouldBe true
    }

    "MosaicPointJTS" should "not fail for empty Seq" in {
        val expected = MosaicPointJTS.fromWKT(
            "POINT EMPTY"
        )
        val actual = MosaicPointJTS.fromSeq(Seq())
        expected.equals(actual) shouldBe true
    }

    "MosaicPointJTS" should "read all supported formats" in {
        val point = MosaicPointJTS.fromWKT("POINT(1 1)")
        noException should be thrownBy MosaicPointJTS.fromWKB(point.toWKB)
        noException should be thrownBy MosaicPointJTS.fromHEX(point.toHEX)
        noException should be thrownBy MosaicPointJTS.fromJSON(point.toJSON)
        noException should be thrownBy MosaicPointJTS.fromInternal(point.toInternal.serialize.asInstanceOf[InternalRow])
        point.equals(MosaicPointJTS.fromWKB(point.toWKB)) shouldBe true
        point.equals(MosaicPointJTS.fromHEX(point.toHEX)) shouldBe true
        point.equals(MosaicPointJTS.fromJSON(point.toJSON)) shouldBe true
        point.equals(MosaicPointJTS.fromInternal(point.toInternal.serialize.asInstanceOf[InternalRow])) shouldBe true
    }

    "MosaicPointJTS" should "maintain SRID across operations" in {
        val srid = 32632
        val point = MosaicPointJTS.fromWKT("POINT(1 1)").asInstanceOf[MosaicPointJTS]
        val anotherPoint = MosaicPointJTS.fromWKT("POINT(1 1)").asInstanceOf[MosaicPointJTS]
        val poly = MosaicPolygonJTS.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")

        point.setSpatialReference(srid)

        // MosaicGeometryJTS
        point.buffer(2d).getSpatialReference shouldBe srid
        point.convexHull.getSpatialReference shouldBe srid
        point.getCentroid.getSpatialReference shouldBe srid
        point.intersection(poly).getSpatialReference shouldBe srid
        point.rotate(45).getSpatialReference shouldBe srid
        point.scale(2d, 2d).getSpatialReference shouldBe srid
        point.simplify(0.001).getSpatialReference shouldBe srid
        point.translate(2d, 2d).getSpatialReference shouldBe srid
        point.union(anotherPoint).getSpatialReference shouldBe srid

        // MosaicPointJTS
        point.getBoundary.getSpatialReference shouldBe srid
        point.mapXY({ (x: Double, y: Double) => (x * 2, y / 2) }).getSpatialReference shouldBe srid
    }

    "MosaicPointJTS" should "correctly apply CRS transformation" in {
        val sridSource = 4326
        val sridTarget = 27700
        val testPoint = MosaicPointJTS.fromWKT("POINT(-0.1390688 51.5178267)").asInstanceOf[MosaicPointJTS]
        testPoint.setSpatialReference(sridSource)
        val expectedResult = MosaicPointJTS.fromWKT("POINT(529217.26 181519.64)").asInstanceOf[MosaicPointJTS]
        val testResult = testPoint.transformCRSXY(sridTarget).asInstanceOf[MosaicPointJTS]
        val comparison = {
            expectedResult.asSeq
                .zip(testResult.asSeq)
                .map({ case (a: Double, b: Double) => math.abs(a - b) <= 0.01 })
        }
        comparison.take(2) should contain only true
    }

}
