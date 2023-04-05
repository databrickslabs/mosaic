package com.databricks.labs.mosaic.core.geometry.point

import com.databricks.labs.mosaic.core.geometry.polygon.MosaicPolygonESRI
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.catalyst.InternalRow

class TestPointESRI extends AnyFlatSpec {

    "MosaicPointESRI" should "return Nil for holes and hole points calls." in {
        val point = MosaicPointESRI.fromWKT("POINT(1 1)")
        point.getHoles shouldEqual Nil
        point.getHolePoints shouldEqual Nil
    }

    "MosaicPointESRI" should "return seq(this) for shells and flatten calls." in {
        val point = MosaicPointESRI.fromWKT("POINT(1 1)")
        the[Exception] thrownBy
            point.getShells should have
        message("getShells should not be called on MultiPoints.")
        point.flatten should contain theSameElementsAs Seq(point)
    }

    "MosaicPointESRI" should "return number of points." in {
        val point = MosaicPointESRI.fromWKT("POINT(1 1)")
        point.numPoints shouldEqual 1
    }

    "MosaicPointESRI" should "be instantiable from a Seq of MosaicPointESRI" in {
        val expected = MosaicPointESRI.fromWKT("POINT (1 1)")
        val pointsSeq = Seq("POINT (1 1)")
          .map(MosaicPointESRI.fromWKT)
          .map(_.asInstanceOf[MosaicPointESRI])
        val actual = MosaicPointESRI.fromSeq(pointsSeq)
        expected.equals(actual) shouldBe true
    }

    "MosaicPointESRI" should "not fail for empty Seq" in {
        val expected = MosaicPointESRI.fromWKT(
            "POINT EMPTY"
        )
        val actual = MosaicPointESRI.fromSeq(Seq())
        expected.equals(actual) shouldBe true
    }

    "MosaicPointESRI" should "read all supported formats" in {
        val point = MosaicPointESRI.fromWKT("POINT(1 1)")
        noException should be thrownBy MosaicPointESRI.fromWKB(point.toWKB)
        noException should be thrownBy MosaicPointESRI.fromHEX(point.toHEX)
        noException should be thrownBy MosaicPointESRI.fromJSON(point.toJSON)
        noException should be thrownBy MosaicPointESRI.fromInternal(point.toInternal.serialize.asInstanceOf[InternalRow])
        point.equals(MosaicPointESRI.fromWKB(point.toWKB)) shouldBe true
        point.equals(MosaicPointESRI.fromHEX(point.toHEX)) shouldBe true
        point.equals(MosaicPointESRI.fromJSON(point.toJSON)) shouldBe true
        point.equals(MosaicPointESRI.fromInternal(point.toInternal.serialize.asInstanceOf[InternalRow])) shouldBe true
    }

    "MosaicPointESRI" should "maintain SRID across operations" in {
        val srid = 32632
        val point = MosaicPointESRI.fromWKT("POINT(1 1)").asInstanceOf[MosaicPointESRI]
        val anotherPoint = MosaicPointESRI.fromWKT("POINT(1 1)").asInstanceOf[MosaicPointESRI]
        val poly = MosaicPolygonESRI.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")

        point.setSpatialReference(srid)

        // geometry
        point.buffer(2d).getSpatialReference shouldBe srid
        point.convexHull.getSpatialReference shouldBe srid
        point.getCentroid.getSpatialReference shouldBe srid
        point.intersection(poly).getSpatialReference shouldBe srid
        point.rotate(45).getSpatialReference shouldBe srid
        point.scale(2d, 2d).getSpatialReference shouldBe srid
        point.simplify(0.001).getSpatialReference shouldBe srid
        point.translate(2d, 2d).getSpatialReference shouldBe srid
        point.union(anotherPoint).getSpatialReference shouldBe srid

        // point
        point.getBoundary.getSpatialReference shouldBe srid
        point.mapXY({ (x: Double, y: Double) => (x * 2, y / 2) }).getSpatialReference shouldBe srid
    }

    "MosaicPointESRI" should "correctly apply CRS transformation" in {
        val sridSource = 4326
        val sridTarget = 27700
        val testPoint = MosaicPointESRI.fromWKT("POINT(-0.1390688 51.5178267)").asInstanceOf[MosaicPointESRI]
        testPoint.setSpatialReference(sridSource)
        val expectedResult = MosaicPointESRI.fromWKT("POINT(529217.26 181519.64)").asInstanceOf[MosaicPointESRI]
        val testResult = testPoint.transformCRSXY(sridTarget).asInstanceOf[MosaicPointESRI]
        val comparison = {
            expectedResult.asSeq
                .zip(testResult.asSeq)
                .map({ case (a: Double, b: Double) => math.abs(a - b) <= 0.01 })
        }
        comparison should contain only true
    }

}
