package com.databricks.labs.mosaic.core.geometry.api

import com.databricks.labs.mosaic.core.geometry.{MosaicGeometry, MosaicPoint}
import com.databricks.labs.mosaic.core.types.{GeoJSONType, GeometryTypeEnum, HexType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{BinaryType, DataType, DateType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class GeometryAPITest extends AnyFunSuite with MockFactory {

  val mockReader: GeometryReader = mock[GeometryReader]

  abstract class TestGeometryAPI extends GeometryAPI(mockReader) {
    override final def pointsToGeometry(points: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value): MosaicGeometry = super.pointsToGeometry(points, geomType)

    override final def rowToGeometry(inputData: InternalRow, dataType: DataType): MosaicGeometry = super.rowToGeometry(inputData, dataType)

    override final def valueToGeometry(inputData: Any, dataType: DataType): MosaicGeometry = super.valueToGeometry(inputData, dataType)

    override final def serialize(geometry: MosaicGeometry, dataType: DataType): Any = super.serialize(geometry, dataType)
  }

  val mockApi: TestGeometryAPI = mock[TestGeometryAPI]
  val mockRow: InternalRow = mock[InternalRow]
  val mockPoint: MosaicPoint = mock[MosaicPoint]
  val bytes: Array[Byte] = "POINT (1 1)".getBytes
  val subRow: InternalRow = InternalRow.fromSeq(Seq(UTF8String.fromString("POINT (1 1)")))

  def doMock(): Unit = {
    mockRow.getString _ expects 0 returning "POINT (1 1)" anyNumberOfTimes()
    mockRow.getBinary _ expects 0 returning bytes anyNumberOfTimes()
    mockRow.get _ expects(0, HexType) returning subRow anyNumberOfTimes()
    mockRow.get _ expects(0, GeoJSONType) returning subRow anyNumberOfTimes()
    mockReader.fromWKB _ expects bytes returning mock[MosaicPoint] anyNumberOfTimes()
    mockReader.fromWKT _ expects "POINT (1 1)" returning mock[MosaicPoint] anyNumberOfTimes()
    mockReader.fromHEX _ expects "POINT (1 1)" returning mock[MosaicPoint] anyNumberOfTimes()
    mockReader.fromJSON _ expects "POINT (1 1)" returning mock[MosaicPoint] anyNumberOfTimes()
    mockReader.fromSeq _ expects(Seq(mockPoint), GeometryTypeEnum.POINT) returning mock[MosaicPoint] anyNumberOfTimes()
    mockPoint.toWKT _ expects() returning "POINT (1 1)" anyNumberOfTimes()
    mockPoint.toWKB _ expects() returning bytes anyNumberOfTimes()
    mockPoint.toJSON _ expects() returning "POINT (1 1)" anyNumberOfTimes()
    mockPoint.toHEX _ expects() returning "POINT (1 1)" anyNumberOfTimes()
  }

  test("GeometryAPI should convert points to geometry") {
    doMock()

    mockApi.pointsToGeometry(Seq(mockPoint), GeometryTypeEnum.POINT) shouldBe a[MosaicGeometry]
  }

  test("GeometryAPI should convert row to geometry") {
    doMock()

    mockApi.rowToGeometry(mockRow, BinaryType) shouldBe a[MosaicGeometry]
    mockApi.rowToGeometry(mockRow, StringType) shouldBe a[MosaicGeometry]
    mockApi.rowToGeometry(mockRow, GeoJSONType) shouldBe a[MosaicGeometry]
    mockApi.rowToGeometry(mockRow, HexType) shouldBe a[MosaicGeometry]
    an[Error] should be thrownBy mockApi.rowToGeometry(mockRow, DateType)
  }

  test("GeometryAPI should convert value to geometry") {
    doMock()

    mockApi.valueToGeometry(bytes, BinaryType) shouldBe a[MosaicGeometry]
    mockApi.valueToGeometry(UTF8String.fromString("POINT (1 1)"), StringType) shouldBe a[MosaicGeometry]
    mockApi.valueToGeometry(subRow, GeoJSONType) shouldBe a[MosaicGeometry]
    mockApi.valueToGeometry(subRow, HexType) shouldBe a[MosaicGeometry]
    an[Error] should be thrownBy mockApi.valueToGeometry("POINT (1 1)", DateType)
  }

  test("GeometryAPI should serialize geometry") {
    doMock()

    mockApi.serialize(mockPoint, BinaryType) shouldBe a[Array[Byte]]
    mockApi.serialize(mockPoint, StringType) shouldBe a[UTF8String]
    mockApi.serialize(mockPoint, GeoJSONType) shouldBe a[InternalRow]
    mockApi.serialize(mockPoint, HexType) shouldBe a[InternalRow]
    an[Error] should be thrownBy mockApi.serialize(mockPoint, DateType)
  }

}
