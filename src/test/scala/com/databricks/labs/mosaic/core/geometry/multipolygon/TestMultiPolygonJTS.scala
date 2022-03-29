package com.databricks.labs.mosaic.core.geometry.multipolygon

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

}
