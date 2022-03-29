package com.databricks.labs.mosaic.core.geometry.multipoint

import com.databricks.labs.mosaic.core.geometry.point.MosaicPointESRI
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.catalyst.InternalRow

class TestMultiPointESRI extends AnyFlatSpec {

    "MosaicMultiPointESRI" should "return Nil for holes and hole points calls." in {
        val multiPoint = MosaicMultiPointESRI.fromWKT("MULTIPOINT (1 1, 2 2, 3 3)")
        multiPoint.getHoles shouldEqual Nil
        multiPoint.getHolePoints shouldEqual Nil
    }

    "MosaicMultiPointESRI" should "return seq(this) for shells and flatten calls." in {
        val multiPoint = MosaicMultiPointESRI.fromWKT("MULTIPOINT (1 1, 2 2, 3 3)")
        val point = MosaicPointESRI.fromWKT("POINT (1 1)")
        the[Exception] thrownBy
            multiPoint.getShells.head.equals(point) should have
        message("getShells should not be called on MultiPoints.")
        multiPoint.flatten.head.equals(point) shouldBe true
    }

    "MosaicMultiPointESRI" should "return number of points." in {
        val multiPoint = MosaicMultiPointESRI.fromWKT("MULTIPOINT (1 1, 2 2, 3 3)")
        multiPoint.numPoints shouldEqual 3
    }

    "MosaicMultiPointESRI" should "read all supported formats" in {
        val multiPoint = MosaicMultiPointESRI.fromWKT("MULTIPOINT (1 1, 2 2, 3 3)")
        noException should be thrownBy MosaicMultiPointESRI.fromWKB(multiPoint.toWKB)
        noException should be thrownBy MosaicMultiPointESRI.fromHEX(multiPoint.toHEX)
        noException should be thrownBy MosaicMultiPointESRI.fromJSON(multiPoint.toJSON)
        noException should be thrownBy MosaicMultiPointESRI.fromInternal(multiPoint.toInternal.serialize.asInstanceOf[InternalRow])
        multiPoint.equals(MosaicMultiPointESRI.fromWKB(multiPoint.toWKB)) shouldBe true
        multiPoint.equals(MosaicMultiPointESRI.fromHEX(multiPoint.toHEX)) shouldBe true
        multiPoint.equals(MosaicMultiPointESRI.fromJSON(multiPoint.toJSON)) shouldBe true
        multiPoint.equals(MosaicMultiPointESRI.fromInternal(multiPoint.toInternal.serialize.asInstanceOf[InternalRow])) shouldBe true
    }

}
