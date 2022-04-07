package com.databricks.labs.mosaic.core.geometry.point

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

}
