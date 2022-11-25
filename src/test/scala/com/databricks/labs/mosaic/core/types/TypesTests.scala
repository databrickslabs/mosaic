package com.databricks.labs.mosaic.core.types

import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
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
        val h3CellId = H3IndexSystem.pointToIndex(10, 10, 10)
        val h3HexCellId = f"$h3CellId%x"
        val bngCellId = BNGIndexSystem.pointToIndex(1000, 1000, 4)
        val bngStrCellId = BNGIndexSystem.format(bngCellId)

        val h3Chip = MosaicChip(isCore = true, index = Left(h3CellId), geom = null)
        val h3HexChip = MosaicChip(isCore = true, index = Right(h3HexCellId), geom = null)
        val bngChip = MosaicChip(isCore = true, index = Left(bngCellId), geom = null)
        val bngStrChip = MosaicChip(isCore = true, index = Right(bngStrCellId), geom = null)

        H3IndexSystem.setCellIdDataType(BooleanType)
        an[IllegalArgumentException] should be thrownBy h3Chip.formatCellId(H3IndexSystem)

        H3IndexSystem.setCellIdDataType(StringType)
        h3Chip.formatCellId(H3IndexSystem).index.right.get shouldBe h3HexCellId
        h3HexChip.formatCellId(H3IndexSystem).index.right.get shouldBe h3HexCellId
        h3Chip.cellIdAsStr(H3IndexSystem) shouldBe h3HexCellId
        h3HexChip.cellIdAsStr(H3IndexSystem) shouldBe h3HexCellId

        H3IndexSystem.setCellIdDataType(LongType)
        h3Chip.formatCellId(H3IndexSystem).index.left.get shouldBe h3CellId
        h3HexChip.formatCellId(H3IndexSystem).index.left.get shouldBe h3CellId
        h3Chip.cellIdAsLong(H3IndexSystem) shouldBe h3CellId
        h3HexChip.cellIdAsLong(H3IndexSystem) shouldBe h3CellId

        BNGIndexSystem.setCellIdDataType(BooleanType)
        an[IllegalArgumentException] should be thrownBy bngChip.formatCellId(BNGIndexSystem)

        BNGIndexSystem.setCellIdDataType(LongType)
        bngChip.formatCellId(BNGIndexSystem).index.left.get shouldBe bngCellId
        bngStrChip.formatCellId(BNGIndexSystem).index.left.get shouldBe bngCellId
        bngChip.cellIdAsLong(BNGIndexSystem) shouldBe bngCellId
        bngStrChip.cellIdAsLong(BNGIndexSystem) shouldBe bngCellId

        BNGIndexSystem.setCellIdDataType(StringType)
        bngChip.formatCellId(BNGIndexSystem).index.right.get shouldBe bngStrCellId
        bngStrChip.formatCellId(BNGIndexSystem).index.right.get shouldBe bngStrCellId
        bngChip.cellIdAsStr(BNGIndexSystem) shouldBe bngStrCellId
        bngStrChip.cellIdAsStr(BNGIndexSystem) shouldBe bngStrCellId

    }

}
