package com.databricks.labs.mosaic.core.types

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.index.IndexSystem
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{BinaryType, LongType, StringType}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class MosaicChipTest extends AnyFunSuite with MockFactory {

  test("ChipType") {
    val mockMosaicGeometry = mock[MosaicGeometry]
    val mockIndexSystem = mock[IndexSystem]
    mockIndexSystem.format _ expects 1L returning "1" anyNumberOfTimes()
    mockIndexSystem.parse _ expects "1" returning 1L anyNumberOfTimes()
    mockMosaicGeometry.toWKB _ expects() returning Array[Byte](1, 2, 3) anyNumberOfTimes()

    MosaicChip(isCore = false, Left(1L), null).isEmpty shouldBe true
    MosaicChip(isCore = true, Left(1L), null).isEmpty shouldBe false

    mockIndexSystem.getCellIdDataType _ expects() returning LongType once()
    MosaicChip(isCore = false, Left(1L), null).formatCellId(mockIndexSystem) shouldBe MosaicChip(isCore = false, Left(1L), null)

    mockIndexSystem.getCellIdDataType _ expects() returning StringType once()
    MosaicChip(isCore = false, Left(1L), null).formatCellId(mockIndexSystem) shouldBe MosaicChip(isCore = false, Right("1"), null)

    mockIndexSystem.getCellIdDataType _ expects() returning LongType once()
    MosaicChip(isCore = false, Right("1"), null).formatCellId(mockIndexSystem) shouldBe MosaicChip(isCore = false, Left(1L), null)

    mockIndexSystem.getCellIdDataType _ expects() returning StringType once()
    MosaicChip(isCore = false, Right("1"), null).formatCellId(mockIndexSystem) shouldBe MosaicChip(isCore = false, Right("1"), null)

    mockIndexSystem.getCellIdDataType _ expects() returning BinaryType once()
    an[IllegalArgumentException] should be thrownBy MosaicChip(isCore = false, Left(1L), null).formatCellId(mockIndexSystem)

    MosaicChip(isCore = false, Left(1L), null).cellIdAsLong(mockIndexSystem) shouldBe 1L
    MosaicChip(isCore = false, Right("1"), null).cellIdAsLong(mockIndexSystem) shouldBe 1L

    MosaicChip(isCore = false, Left(1L), null).cellIdAsStr(mockIndexSystem) shouldBe "1"
    MosaicChip(isCore = false, Right("1"), null).cellIdAsStr(mockIndexSystem) shouldBe "1"

    MosaicChip(isCore = false, Left(1L), null).serialize shouldBe a[InternalRow]
    MosaicChip(isCore = false, Right("1"), null).serialize shouldBe a[InternalRow]

    MosaicChip(isCore = false, Left(1L), mockMosaicGeometry).serialize shouldBe a[InternalRow]

  }

}
