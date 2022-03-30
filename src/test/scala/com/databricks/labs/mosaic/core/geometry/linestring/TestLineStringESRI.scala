package com.databricks.labs.mosaic.core.geometry.linestring

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.catalyst.InternalRow

class TestLineStringESRI extends AnyFlatSpec {

    "MosaicLineStringESRI" should "throw exceptions for function calls when empty construct is used" in {
        val lineString = new MosaicLineStringESRI()
        an[NullPointerException] should be thrownBy lineString.numPoints
    }

    "MosaicLineStringESRI" should "return Nil for holes and hole points calls." in {
        val lineString = MosaicLineStringESRI.fromWKT("LINESTRING (1 1, 2 2, 3 3)")
        lineString.getHoles shouldEqual Nil
        lineString.getHolePoints shouldEqual Nil
    }

    "MosaicLineStringESRI" should "return seq(this) for shells and flatten calls." in {
        val lineString = MosaicLineStringESRI.fromWKT("LINESTRING (1 1, 2 2, 3 3)")
        lineString.getShells.head shouldEqual lineString
        lineString.flatten.head shouldEqual lineString
    }

    "MosaicLineStringESRI" should "return number of points." in {
        val lineString = MosaicLineStringESRI.fromWKT("LINESTRING (1 1, 2 2, 3 3)")
        lineString.numPoints shouldEqual 3
    }

    "MosaicLineStringESRI" should "read all supported formats" in {
        val lineString = MosaicLineStringESRI.fromWKT("LINESTRING (1 1, 2 2, 3 3)")
        noException should be thrownBy MosaicLineStringESRI.fromWKB(lineString.toWKB)
        noException should be thrownBy MosaicLineStringESRI.fromHEX(lineString.toHEX)
        noException should be thrownBy MosaicLineStringESRI.fromJSON(lineString.toJSON)
        noException should be thrownBy MosaicLineStringESRI.fromInternal(lineString.toInternal.serialize.asInstanceOf[InternalRow])
        lineString.equals(MosaicLineStringESRI.fromWKB(lineString.toWKB)) shouldBe true
        lineString.equals(MosaicLineStringESRI.fromHEX(lineString.toHEX)) shouldBe true
        lineString.equals(MosaicLineStringESRI.fromJSON(lineString.toJSON)) shouldBe true
        lineString.equals(MosaicLineStringESRI.fromInternal(lineString.toInternal.serialize.asInstanceOf[InternalRow])) shouldBe true
    }

//    "MosaicLineStringESRI" should "read all supported formats" in {}

}
