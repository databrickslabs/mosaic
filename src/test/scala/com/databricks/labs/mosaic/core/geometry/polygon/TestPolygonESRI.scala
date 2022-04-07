package com.databricks.labs.mosaic.core.geometry.polygon

import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineStringESRI
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.catalyst.InternalRow

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

}
