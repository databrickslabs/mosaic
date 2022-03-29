package com.databricks.labs.mosaic.core.geometry.linestring

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.catalyst.InternalRow

class TestLineStringJTS extends AnyFlatSpec {

    "MosaicLineStringJTS" should "return Nil for holes and hole points calls." in {
        val lineString = MosaicLineStringJTS.fromWKT("LINESTRING (1 1, 2 2, 3 3)")
        lineString.getHoles shouldEqual Nil
        lineString.getHolePoints shouldEqual Nil
    }

    "MosaicLineStringJTS" should "return seq(this) for shells and flatten calls." in {
        val lineString = MosaicLineStringJTS.fromWKT("LINESTRING (1 1, 2 2, 3 3)")
        lineString.getShells.head shouldEqual lineString
        lineString.flatten.head shouldEqual lineString
    }

    "MosaicLineStringJTS" should "return number of points." in {
        val lineString = MosaicLineStringJTS.fromWKT("LINESTRING (1 1, 2 2, 3 3)")
        lineString.numPoints shouldEqual 3
    }

    "MosaicLineStringJTS" should "read all supported formats" in {
        val lineString = MosaicLineStringJTS.fromWKT("LINESTRING (1 1, 2 2, 3 3)")
        noException should be thrownBy MosaicLineStringJTS.fromWKB(lineString.toWKB)
        noException should be thrownBy MosaicLineStringJTS.fromHEX(lineString.toHEX)
        noException should be thrownBy MosaicLineStringJTS.fromJSON(lineString.toJSON)
        noException should be thrownBy MosaicLineStringJTS.fromInternal(lineString.toInternal.serialize.asInstanceOf[InternalRow])
        lineString.equals(MosaicLineStringJTS.fromWKB(lineString.toWKB)) shouldBe true
        lineString.equals(MosaicLineStringJTS.fromHEX(lineString.toHEX)) shouldBe true
        lineString.equals(MosaicLineStringJTS.fromJSON(lineString.toJSON)) shouldBe true
        lineString.equals(MosaicLineStringJTS.fromInternal(lineString.toInternal.serialize.asInstanceOf[InternalRow])) shouldBe true
    }

}
