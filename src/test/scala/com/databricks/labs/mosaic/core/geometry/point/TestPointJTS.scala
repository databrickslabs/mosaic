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
        point.reduceFromMulti.getSpatialReference shouldBe srid
        point.rotate(45).getSpatialReference shouldBe srid
        point.scale(2d, 2d).getSpatialReference shouldBe srid
        point.simplify(0.001).getSpatialReference shouldBe srid
        point.translate(2d, 2d).getSpatialReference shouldBe srid
        point.union(anotherPoint).getSpatialReference shouldBe srid

        // MosaicPointJTS
        point.getBoundary.getSpatialReference shouldBe srid
        point.mapXY({ (x: Double, y: Double) => (x * 2, y / 2) }).getSpatialReference shouldBe srid
    }

}
