package com.databricks.labs.mosaic.core.types

import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.core.types.model.MosaicChip
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.{an, convertToAnyShouldWrapper}

class TypesTests extends AnyFunSuite {


    test("TypeNames should be like expected") {
        assert(HexType.simpleString == "HEX")
        assert(JSONType.simpleString == "GEOJSON")
        assert(InternalGeometryType.simpleString == "COORDS")
        assert(ChipType(LongType).simpleString == "CHIP")
        assert(ChipType(IntegerType).simpleString == "CHIP")
        assert(ChipType(StringType).simpleString == "CHIP")
        assert(MosaicType(LongType).simpleString == "MOSAIC")
        assert(MosaicType(IntegerType).simpleString == "MOSAIC")
        assert(MosaicType(StringType).simpleString == "MOSAIC")
    }

    test("MosaicChip behavior") {
        val cellId = H3IndexSystem.pointToIndex(10, 10, 10)
        val hexCellId = f"$cellId%x"

        val chip = MosaicChip(isCore = true, index = Left(cellId), geom = null)
        val hexChip = MosaicChip(isCore = true, index = Right(hexCellId), geom = null)

        H3IndexSystem.setCellIdDataType(BooleanType)
        an[IllegalArgumentException] should be thrownBy chip.formatCellId(H3IndexSystem)


        H3IndexSystem.setCellIdDataType(StringType)
        chip.formatCellId(H3IndexSystem).index.right.get shouldBe hexCellId
        hexChip.formatCellId(H3IndexSystem).index.right.get shouldBe hexCellId
        chip.cellIdAsStr(H3IndexSystem) shouldBe hexCellId
        hexChip.cellIdAsStr(H3IndexSystem) shouldBe hexCellId

        H3IndexSystem.setCellIdDataType(LongType)
        chip.formatCellId(H3IndexSystem).index.left.get shouldBe cellId
        hexChip.formatCellId(H3IndexSystem).index.left.get shouldBe cellId
        chip.cellIdAsLong(H3IndexSystem) shouldBe cellId
        hexChip.cellIdAsLong(H3IndexSystem) shouldBe cellId

    }

}
