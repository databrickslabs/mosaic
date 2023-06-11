package com.databricks.labs.mosaic.core.index

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.types.{Coordinates, MosaicChip}
import org.apache.spark.sql.types.{DataType, DateType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class IndexSystemTest extends AnyFunSuite with MockFactory {

  abstract class TestIndexSystem extends IndexSystem(LongType) {
    // Override as final all the methods whose behavior we want to test, this will prevent the compiler from mocking those methods
    override final def getCellIdDataType: DataType = super.getCellIdDataType

    override final def setCellIdDataType(dataType: DataType): Unit = super.setCellIdDataType(dataType)

    override final def formatCellId(cellId: Any, dt: DataType): Any = super.formatCellId(cellId, dt)

    override final def formatCellId(cellId: Any): Any = super.formatCellId(cellId)

    override final def serializeCellId(cellId: Any): Any = super.serializeCellId(cellId)

    override final def kRing(index: String, n: Int): Seq[String] = super.kRing(index, n)

    override final def kLoop(index: String, n: Int): Seq[String] = super.kLoop(index, n)

    override final def getBorderChips(geometry: MosaicGeometry, borderIndices: Seq[Long], keepCoreGeom: Boolean, geometryAPI: GeometryAPI): Seq[MosaicChip] =
      super.getBorderChips(geometry, borderIndices, keepCoreGeom, geometryAPI)

    override final def getCoreChips(coreIndices: Seq[Long], keepCoreGeom: Boolean, geometryAPI: GeometryAPI): Seq[MosaicChip] =
      super.getCoreChips(coreIndices, keepCoreGeom, geometryAPI)

    override final def area(index: Long): Double = super.area(index)

    override final def area(index: String): Double = super.area(index)

    override final def indexToCenter(index: String): Coordinates = super.indexToCenter(index)

    override final def indexToBoundary(index: String): Seq[Coordinates] = super.indexToBoundary(index)

    override final def coerceChipGeometry(geometries: Seq[MosaicGeometry]): Seq[MosaicGeometry] = super.coerceChipGeometry(geometries)

    override final def coerceChipGeometry(geom: MosaicGeometry, cell: Long, geometryAPI: GeometryAPI): MosaicGeometry = super.coerceChipGeometry(geom, cell, geometryAPI)
  }

  val mockIndexSystem: IndexSystem = mock[TestIndexSystem]
  val mockGeometry: MosaicGeometry = mock[MosaicGeometry]
  val mockGeometry2: MosaicGeometry = mock[MosaicGeometry]
  val mockGeometry3: MosaicGeometry = mock[MosaicGeometry]
  val mockGeometry4: MosaicGeometry = mock[MosaicGeometry]
  val mockGeometryAPI: GeometryAPI = mock[GeometryAPI]

  def doMock(): Unit = {
    mockIndexSystem.format _ expects 123456789L returning "123456789" anyNumberOfTimes()
    mockIndexSystem.parse _ expects "123456789" returning 123456789L anyNumberOfTimes()
    mockIndexSystem.parse _ expects "10000000001" returning 10000000001L anyNumberOfTimes()

    (mockIndexSystem.kRing(_: Long, _: Int)) expects(123456789L, 1) returning Seq(123456789L) anyNumberOfTimes()
    (mockIndexSystem.kLoop(_: Long, _: Int)) expects(123456789L, 1) returning Seq(123456789L) anyNumberOfTimes()

    (mockIndexSystem.indexToGeometry(_: Long, _: GeometryAPI)) expects(10000000001L, mockGeometryAPI) returning mockGeometry anyNumberOfTimes()
    (mockIndexSystem.indexToGeometry(_: Long, _: GeometryAPI)) expects(10000000002L, mockGeometryAPI) returning mockGeometry anyNumberOfTimes()
    (mockIndexSystem.indexToGeometry(_: Long, _: GeometryAPI)) expects(10000000003L, mockGeometryAPI) returning mockGeometry2 anyNumberOfTimes()

    (mockIndexSystem.indexToCenter(_: Long)) expects 10000000001L returning Coordinates(1.5, 1.5) anyNumberOfTimes()
    (mockIndexSystem.indexToBoundary(_: Long)) expects 10000000001L returning
      Seq(Coordinates(1, 1), Coordinates(1, 2), Coordinates(2, 2), Coordinates(1, 2), Coordinates(1, 1)) anyNumberOfTimes()

    mockGeometry.intersection _ expects mockGeometry returning mockGeometry anyNumberOfTimes()
    mockGeometry.intersection _ expects mockGeometry2 returning mockGeometry2 anyNumberOfTimes()
    mockGeometry.getGeometryType _ expects() returning "POINT" anyNumberOfTimes()
    mockGeometry2.getGeometryType _ expects() returning "GEOMETRYCOLLECTION" anyNumberOfTimes()
    mockGeometry3.getGeometryType _ expects() returning "POLYGON" anyNumberOfTimes()
    mockGeometry4.getGeometryType _ expects() returning "LINESTRING" anyNumberOfTimes()
    mockGeometry.equals _ expects mockGeometry returning true anyNumberOfTimes()
    mockGeometry2.equals _ expects mockGeometry returning false anyNumberOfTimes()
    mockGeometry2.equals _ expects mockGeometry2 returning false anyNumberOfTimes()
    mockGeometry.equals _ expects mockGeometry2 returning false anyNumberOfTimes()
    mockGeometry.isEmpty _ expects() returning false anyNumberOfTimes()
    mockGeometry2.isEmpty _ expects() returning false anyNumberOfTimes()
    mockGeometry2.getBoundary _ expects() returning mockGeometry2 anyNumberOfTimes()
    mockGeometry.difference _ expects mockGeometry2 returning mockGeometry anyNumberOfTimes()
    mockGeometry2.difference _ expects mockGeometry2 returning mockGeometry anyNumberOfTimes()

  }

  test("IndexSystem should get and set cellID data type") {
    mockIndexSystem.getCellIdDataType shouldBe LongType
    noException should be thrownBy mockIndexSystem.setCellIdDataType(LongType)
  }

  test("IndexSystem should format cellID") {
    doMock()
    mockIndexSystem.formatCellId(123456789L, LongType) shouldBe 123456789L
    mockIndexSystem.formatCellId("123456789", LongType) shouldBe 123456789L
    mockIndexSystem.formatCellId(UTF8String.fromString("123456789"), LongType) shouldBe 123456789L
    mockIndexSystem.formatCellId(123456789L, StringType) shouldBe "123456789"
    mockIndexSystem.formatCellId("123456789", StringType) shouldBe "123456789"
    mockIndexSystem.formatCellId(UTF8String.fromString("123456789"), StringType) shouldBe "123456789"

    an[Error] should be thrownBy mockIndexSystem.formatCellId(123456789L, DateType)

    mockIndexSystem.formatCellId(123456789L) shouldBe 123456789L
  }

  test("IndexSystem should serializeCellId") {
    doMock()

    mockIndexSystem.setCellIdDataType(StringType)
    mockIndexSystem.getCellIdDataType shouldBe StringType

    mockIndexSystem.serializeCellId(123456789L) shouldBe UTF8String.fromString("123456789")
    mockIndexSystem.serializeCellId("123456789") shouldBe UTF8String.fromString("123456789")
    mockIndexSystem.serializeCellId(UTF8String.fromString("123456789")) shouldBe UTF8String.fromString("123456789")

    mockIndexSystem.setCellIdDataType(LongType)
    mockIndexSystem.getCellIdDataType shouldBe LongType

    mockIndexSystem.serializeCellId(123456789L) shouldBe 123456789L
    mockIndexSystem.serializeCellId("123456789") shouldBe 123456789L
    mockIndexSystem.serializeCellId(UTF8String.fromString("123456789")) shouldBe 123456789L


    an[Error] should be thrownBy mockIndexSystem.serializeCellId(1.0)
  }

  test("IndexSystem should implement kRing and kLoop for StringType") {
    doMock()
    mockIndexSystem.kRing("123456789", 1) shouldBe a[Seq[_]]
    mockIndexSystem.kLoop("123456789", 1) shouldBe a[Seq[_]]
  }

  test("IndexSystem should implement getBorderChips") {
    doMock()

    val result = mockIndexSystem.getBorderChips(
      mockGeometry,
      Seq(10000000001L, 10000000002L, 10000000003L),
      keepCoreGeom = true,
      mockGeometryAPI
    )

    result shouldBe a[Seq[_]]
    result.last shouldBe a[MosaicChip]
    result.last.index.left.get shouldBe 10000000003L

    val result2 = mockIndexSystem.getBorderChips(
      mockGeometry,
      Seq(10000000001L, 10000000002L, 10000000003L),
      keepCoreGeom = false,
      mockGeometryAPI
    )

    result2.map(_.geom).flatMap(Option(_)).length shouldBe 1
  }

  test("IndexSystem should implement getCoreChips") {
    doMock()

    val result = mockIndexSystem.getCoreChips(
      Seq(10000000001L, 10000000002L, 10000000003L),
      keepCoreGeom = true,
      mockGeometryAPI
    )

    result shouldBe a[Seq[_]]
    result.last shouldBe a[MosaicChip]
    result.last.index.left.get shouldBe 10000000003L

    val result2 = mockIndexSystem.getCoreChips(
      Seq(10000000001L, 10000000002L, 10000000003L),
      keepCoreGeom = false,
      mockGeometryAPI
    )

    result2.map(_.geom).flatMap(Option(_)).length shouldBe 0
  }

  test("IndexSystem should implement area") {
    doMock()
    mockIndexSystem.area(10000000001L) shouldBe 12360.971936046964
    mockIndexSystem.area("10000000001") shouldBe 12360.971936046964
  }

  test("IndexSystem should implement indexToCenter and indexToBoundary for string type") {
    doMock()
    mockIndexSystem.indexToCenter("10000000001")
    mockIndexSystem.indexToBoundary("10000000001")
  }

  test("IndexSystem should implement coerceChipGeometry") {
    doMock()
    mockIndexSystem.coerceChipGeometry(Seq(mockGeometry, mockGeometry2)) shouldBe Seq(mockGeometry)
    mockIndexSystem.coerceChipGeometry(Seq(mockGeometry, mockGeometry3)) shouldBe Seq(mockGeometry3)
    mockIndexSystem.coerceChipGeometry(Seq(mockGeometry, mockGeometry4)) shouldBe Seq(mockGeometry4)
    mockIndexSystem.coerceChipGeometry(Nil) shouldBe Nil
  }

}
