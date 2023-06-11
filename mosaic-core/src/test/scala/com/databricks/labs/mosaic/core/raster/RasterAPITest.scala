package com.databricks.labs.mosaic.core.raster

import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class RasterAPITest extends AnyFunSuite with MockFactory {

  val mockRasterReader: RasterReader = mock[RasterReader]
  val mockMosaicRaster: MosaicRaster = mock[MosaicRaster]
  val mockMosaicRasterBand: MosaicRasterBand = mock[MosaicRasterBand]

  abstract class TestRasterAPI extends RasterAPI(mockRasterReader) {
    override final def raster(path: String): MosaicRaster = super.raster(path)

    override final def band(path: String, bandIndex: Int): MosaicRasterBand = super.band(path, bandIndex)

    override final def toWorldCoord(gt: Seq[Double], x: Int, y: Int): (Double, Double) =
      super.toWorldCoord(gt, x, y)

    override final def fromWorldCoord(gt: Seq[Double], x: Double, y: Double): (Int, Int) =
      super.fromWorldCoord(gt, x, y)
  }

  test("RasterAPI") {
    val mockRasterAPI = mock[TestRasterAPI]

    mockRasterReader.readRaster _ expects "path" returning mockMosaicRaster anyNumberOfTimes()
    mockRasterReader.readBand _ expects("path", 1) returning mockMosaicRasterBand anyNumberOfTimes()
    mockRasterReader.toWorldCoord _ expects(Seq(1.0, 2.0, 3.0, 4.0, 5.0, 6.0), 1, 1) returning (3.0, 4.0) anyNumberOfTimes()
    mockRasterReader.fromWorldCoord _ expects(Seq(1.0, 2.0, 3.0, 4.0, 5.0, 6.0), 3.0, 4.0) returning (1, 1) anyNumberOfTimes()

    mockRasterAPI.raster("path") shouldBe mockMosaicRaster
    mockRasterAPI.band("path", 1) shouldBe mockMosaicRasterBand

    mockRasterAPI.toWorldCoord(Seq(1.0, 2.0, 3.0, 4.0, 5.0, 6.0), 1, 1) shouldBe (3.0, 4.0)
    mockRasterAPI.fromWorldCoord(Seq(1.0, 2.0, 3.0, 4.0, 5.0, 6.0), 3.0, 4.0) shouldBe (1, 1)
  }

}
