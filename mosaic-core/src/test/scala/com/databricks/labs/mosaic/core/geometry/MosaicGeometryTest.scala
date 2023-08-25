package com.databricks.labs.mosaic.core.geometry

import com.databricks.labs.mosaic.core.crs.{CRSBounds, CRSBoundsProvider}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.{an, convertToAnyShouldWrapper}

import scala.collection.immutable

class MosaicGeometryTest extends AnyFunSuite with MockFactory {

  abstract class TestMosaicGeometry extends MosaicGeometry {
    override final def transformCRSXY(sridTo: Int, sridFrom: Int): MosaicGeometry = super.transformCRSXY(sridTo, sridFrom)

    override final def minMaxCoord(coord: String, minMax: String): Double = super.minMaxCoord(coord, minMax)

    override final def hasValidCoords(crsBoundsProvider: CRSBoundsProvider, crsCode: String, which: String): Boolean = super.hasValidCoords(crsBoundsProvider, crsCode, which)
  }

  class TestCRSBoundsProvider extends CRSBoundsProvider(null) {}

  val mockMosaicGeometry: MosaicGeometry = mock[TestMosaicGeometry]
  val mockMosaicPoints: Seq[immutable.IndexedSeq[MosaicPoint]] = Seq((0 to 5).map(_ => mock[MosaicPoint]))
  val mockCRSBoundsProvider: CRSBoundsProvider = mock[CRSBoundsProvider]
  val mockBounds: CRSBounds = mock[CRSBounds]

  def doMock(): Unit = {
    mockMosaicPoints.foreach(_.zipWithIndex.foreach { case (point, index) =>
      point.getX _ expects() returning index.toDouble anyNumberOfTimes()
      point.getY _ expects() returning index.toDouble anyNumberOfTimes()
      point.getZ _ expects() returning index.toDouble anyNumberOfTimes()
    })
    mockMosaicGeometry.getShellPoints _ expects() returning mockMosaicPoints anyNumberOfTimes()
    mockMosaicGeometry.getHolePoints _ expects() returning Seq.empty anyNumberOfTimes()
    mockCRSBoundsProvider.bounds _ expects("EPSG", "4326".toInt) returning mockBounds anyNumberOfTimes()
    mockCRSBoundsProvider.reprojectedBounds _ expects("EPSG", "4326".toInt) returning mockBounds anyNumberOfTimes()
    mockBounds.getUpperX _ expects() returning 180 anyNumberOfTimes()
    mockBounds.getUpperY _ expects() returning 90 anyNumberOfTimes()
    mockBounds.getLowerX _ expects() returning -180 anyNumberOfTimes()
    mockBounds.getLowerY _ expects() returning -90 anyNumberOfTimes()
  }

  test("MosaicGeometry should return minMaxCoord") {
    doMock()
    mockMosaicGeometry.minMaxCoord("x", "min") shouldBe 0.0
    mockMosaicGeometry.minMaxCoord("x", "max") shouldBe 5.0
    mockMosaicGeometry.minMaxCoord("y", "min") shouldBe 0.0
    mockMosaicGeometry.minMaxCoord("y", "max") shouldBe 5.0
    mockMosaicGeometry.minMaxCoord("z", "min") shouldBe 0.0
    mockMosaicGeometry.minMaxCoord("z", "max") shouldBe 5.0
  }

  test("MosaicGeometry should run hasValidCoords") {
    doMock()
    mockMosaicGeometry.hasValidCoords(mockCRSBoundsProvider, "EPSG:4326", "bounds") shouldBe true
    mockMosaicGeometry.hasValidCoords(mockCRSBoundsProvider, "EPSG:4326", "reprojected_bounds") shouldBe true
    an[Error] should be thrownBy mockMosaicGeometry.hasValidCoords(mockCRSBoundsProvider, "EPSG:4326", "invalid")
  }

  test("MosaicGeometry should fail for transformCRSXY") {
    doMock()
    an[Exception] should be thrownBy mockMosaicGeometry.transformCRSXY(4326, 4326)
  }

}
