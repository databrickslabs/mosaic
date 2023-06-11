package com.databricks.labs.mosaic.core

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.types.MosaicChip
import org.apache.spark.sql.types.LongType
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class MosaicTest extends AnyFunSuite with MockFactory {

  val mockPoint: MosaicPoint = mock[MosaicPoint]
  val mockMultiPoint: MosaicMultiPoint = mock[MosaicMultiPoint]
  val mockLineString: MosaicLineString = mock[MosaicLineString]
  val mockMultiLineString: MosaicMultiLineString = mock[MosaicMultiLineString]
  val mockPolygon: MosaicPolygon = mock[MosaicPolygon]
  val mockIndexSystem: IndexSystem = mock[IndexSystem]
  val mockGeometryAPI: GeometryAPI = mock[GeometryAPI]
  val mockMosaicChip: MosaicChip = mock[MosaicChip]

  def doMock(): Unit = {
    mockPoint.getGeometryType _ expects() returning "POINT" anyNumberOfTimes()
    mockPoint.getX _ expects() returning 1.0 anyNumberOfTimes()
    mockPoint.getY _ expects() returning 1.0 anyNumberOfTimes()
    mockPoint.isEmpty _ expects() returning false anyNumberOfTimes()

    mockMultiPoint.getGeometryType _ expects() returning "MULTIPOINT" anyNumberOfTimes()
    mockMultiPoint.asSeq _ expects() returning Seq(mockPoint) anyNumberOfTimes()

    mockLineString.getGeometryType _ expects() returning "LINESTRING" anyNumberOfTimes()
    mockLineString.getShells _ expects() returning Seq(mockLineString) anyNumberOfTimes()
    mockLineString.asSeq _ expects() returning Seq(mockPoint) anyNumberOfTimes()
    mockLineString.intersection _ expects mockPoint returning mockPoint anyNumberOfTimes()
    mockLineString.buffer _ expects * returning mockPolygon anyNumberOfTimes()

    mockMultiLineString.getGeometryType _ expects() returning "MULTILINESTRING" anyNumberOfTimes()
    mockMultiLineString.flatten _ expects() returning Seq(mockLineString) anyNumberOfTimes()

    mockPolygon.getGeometryType _ expects() returning "POLYGON" anyNumberOfTimes()
    mockPolygon.buffer _ expects * returning mockPolygon anyNumberOfTimes()
    mockPolygon.isEmpty _ expects() returning false anyNumberOfTimes()
    mockPolygon.boundary _ expects() returning mockLineString anyNumberOfTimes()
    mockPolygon.simplify _ expects * returning mockPolygon anyNumberOfTimes()

    mockIndexSystem.pointToIndex _ expects(*, *, *) returning 1L anyNumberOfTimes()
    mockIndexSystem.getCellIdDataType _ expects() returning LongType anyNumberOfTimes()
    (mockIndexSystem.indexToGeometry(_: Long, _: GeometryAPI)) expects(1, mockGeometryAPI) returning mockPoint anyNumberOfTimes()
    (mockIndexSystem.kRing(_: Long, _: Int)) expects(1, 1) returning Seq(1L) anyNumberOfTimes()
    mockIndexSystem.getBufferRadius _ expects(mockPolygon, 1, mockGeometryAPI) returning 1.0 anyNumberOfTimes()
    mockIndexSystem.polyfill _ expects(mockPolygon, 1, Some(mockGeometryAPI)) returning Seq(1L) anyNumberOfTimes()
    mockIndexSystem.getCoreChips _ expects(Seq(1L), false, mockGeometryAPI) returning Seq(mockMosaicChip) anyNumberOfTimes()
    mockIndexSystem.getBorderChips _ expects(mockPolygon, Seq(), false, mockGeometryAPI) returning Seq() anyNumberOfTimes()

    mockMosaicChip.cellIdAsLong _ expects mockIndexSystem returning 1L anyNumberOfTimes()
  }

  test("Mosaic should getChips") {
    doMock()
    val resolution = 1

    Mosaic.getChips(mockPoint, resolution, keepCoreGeom = false, mockIndexSystem, mockGeometryAPI) shouldBe a[Seq[MosaicChip]]
    Mosaic.getChips(mockPoint, resolution, keepCoreGeom = false, mockIndexSystem, mockGeometryAPI).map(_.cellIdAsLong(mockIndexSystem)) should contain theSameElementsAs Seq(1L)

    Mosaic.getChips(mockMultiPoint, resolution, keepCoreGeom = false, mockIndexSystem, mockGeometryAPI) shouldBe a[Seq[MosaicChip]]
    Mosaic.getChips(mockMultiPoint, resolution, keepCoreGeom = false, mockIndexSystem, mockGeometryAPI).map(_.cellIdAsLong(mockIndexSystem)) should contain theSameElementsAs Seq(1L)

    Mosaic.getChips(mockLineString, resolution, keepCoreGeom = false, mockIndexSystem, mockGeometryAPI) shouldBe a[Seq[MosaicChip]]
    Mosaic.getChips(mockLineString, resolution, keepCoreGeom = false, mockIndexSystem, mockGeometryAPI).map(_.cellIdAsLong(mockIndexSystem)) should contain theSameElementsAs Seq(1L)

    Mosaic.getChips(mockMultiLineString, resolution, keepCoreGeom = false, mockIndexSystem, mockGeometryAPI) shouldBe a[Seq[MosaicChip]]
    Mosaic.getChips(mockMultiLineString, resolution, keepCoreGeom = false, mockIndexSystem, mockGeometryAPI).map(_.cellIdAsLong(mockIndexSystem)) should contain theSameElementsAs Seq(1L)

    Mosaic.getChips(mockPolygon, resolution, keepCoreGeom = false, mockIndexSystem, mockGeometryAPI) shouldBe a[Seq[MosaicChip]]
    Mosaic.getChips(mockPolygon, resolution, keepCoreGeom = false, mockIndexSystem, mockGeometryAPI).map(_.cellIdAsLong(mockIndexSystem)) should contain theSameElementsAs Seq(1L)
  }

  test("Mosaic should mosaicFill for empty carved geometries") {
    doMock()
    val resolution = 1

    val mockPolygon2 = mock[MosaicPolygon]
    mockPolygon2.isEmpty _ expects() returning true anyNumberOfTimes()
    mockPolygon2.getGeometryType _ expects() returning "POLYGON" anyNumberOfTimes()
    mockPolygon2.buffer _ expects * returning mockPolygon2 anyNumberOfTimes()
    mockPolygon2.simplify _ expects * returning mockPolygon2 anyNumberOfTimes()
    mockIndexSystem.polyfill _ expects(mockPolygon2, 1, Some(mockGeometryAPI)) returning Seq(1L) anyNumberOfTimes()
    mockIndexSystem.getBufferRadius _ expects(mockPolygon2, 1, mockGeometryAPI) returning 1.0 anyNumberOfTimes()
    mockIndexSystem.getCoreChips _ expects(Seq(1L), true, mockGeometryAPI) returning Seq(mockMosaicChip) anyNumberOfTimes()
    mockIndexSystem.getBorderChips _ expects(mockPolygon2, Seq(), true, mockGeometryAPI) returning Seq() anyNumberOfTimes()


    Mosaic.getChips(mockPolygon2, resolution, keepCoreGeom = true, mockIndexSystem, mockGeometryAPI) shouldBe a[Seq[MosaicChip]]
    Mosaic.getChips(mockPolygon2, resolution, keepCoreGeom = true, mockIndexSystem, mockGeometryAPI).map(_.cellIdAsLong(mockIndexSystem)) should contain theSameElementsAs Seq(1L)
  }

  test("Mosaic should fail for lineFill on polygon") {
    doMock()

    an[Error] should be thrownBy Mosaic.lineFill(mockPolygon, 1, mockIndexSystem, mockGeometryAPI)
  }

  test("Mosaic should implement geometry kRing") {
    doMock()

    (mockIndexSystem.kRing(_: Long, _: Int)) expects(1, 2) returning Seq(1L) anyNumberOfTimes()

    Mosaic.geometryKRing(mockPolygon, 1, 2, mockIndexSystem, mockGeometryAPI) shouldBe a[Set[Long]]
    Mosaic.geometryKRing(mockPolygon, 1, 2, mockIndexSystem, mockGeometryAPI) should contain theSameElementsAs Seq(1L)
  }

  test("Mosaic should implement geometry kLoop") {
    doMock()

    (mockIndexSystem.kLoop(_: Long, _: Int)) expects(1, 2) returning Seq(1L, 2L) anyNumberOfTimes()

    Mosaic.geometryKLoop(mockPolygon, 1, 2, mockIndexSystem, mockGeometryAPI) shouldBe a[Set[Long]]
    Mosaic.geometryKLoop(mockPolygon, 1, 2, mockIndexSystem, mockGeometryAPI) should contain theSameElementsAs Seq(2L)
  }

}
