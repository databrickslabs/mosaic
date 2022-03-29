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

}
