package com.databricks.labs.mosaic.core.geometry.multilinestring

import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineStringJTS
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.catalyst.InternalRow

class TestMultiLineStringJTS extends AnyFlatSpec {

    "MosaicMultiLineStringJTS" should "return Nil for holes and hole points calls." in {
        val multiLineString = MosaicMultiLineStringJTS.fromWKT("MULTILINESTRING ((1 1, 2 2, 3 3), (2 2, 3 3, 4 4))")
        multiLineString.getHoles shouldEqual Nil
        multiLineString.getHolePoints shouldEqual Nil
    }

    "MosaicMultiLineStringJTS" should "return seq(this) for shells and flatten calls." in {
        val multiLineString = MosaicMultiLineStringJTS.fromWKT("MULTILINESTRING ((1 1, 2 2, 3 3), (2 2, 3 3, 4 4))")
        val lineString = MosaicLineStringJTS.fromWKT("LINESTRING(1 1, 2 2, 3 3)")
        multiLineString.getShells.head.equals(lineString) shouldBe true
        multiLineString.flatten.head.equals(lineString) shouldBe true
    }

    "MosaicMultiLineStringJTS" should "return number of points." in {
        val multiLineString = MosaicMultiLineStringJTS.fromWKT("MULTILINESTRING ((1 1, 2 2, 3 3), (2 2, 3 3, 4 4))")
        multiLineString.numPoints shouldEqual 6
    }

    "MosaicMultiLineStringJTS" should "read all supported formats" in {
        val multiLineString = MosaicMultiLineStringJTS.fromWKT("MULTILINESTRING ((1 1, 2 2, 3 3), (2 2, 3 3, 4 4))")
        noException should be thrownBy MosaicMultiLineStringJTS.fromWKB(multiLineString.toWKB)
        noException should be thrownBy MosaicMultiLineStringJTS.fromHEX(multiLineString.toHEX)
        noException should be thrownBy MosaicMultiLineStringJTS.fromJSON(multiLineString.toJSON)
        noException should be thrownBy MosaicMultiLineStringJTS.fromInternal(multiLineString.toInternal.serialize.asInstanceOf[InternalRow])
        multiLineString.equals(MosaicMultiLineStringJTS.fromWKB(multiLineString.toWKB)) shouldBe true
        multiLineString.equals(MosaicMultiLineStringJTS.fromHEX(multiLineString.toHEX)) shouldBe true
        multiLineString.equals(MosaicMultiLineStringJTS.fromJSON(multiLineString.toJSON)) shouldBe true
        multiLineString.equals(MosaicMultiLineStringJTS.fromInternal(multiLineString.toInternal.serialize.asInstanceOf[InternalRow])) shouldBe true
    }

}
