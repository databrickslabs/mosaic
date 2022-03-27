package com.databricks.labs.mosaic.core.geometry.point

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

}
