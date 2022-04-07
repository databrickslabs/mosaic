package com.databricks.labs.mosaic.core.geometry.polygon

import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineStringJTS
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

}
