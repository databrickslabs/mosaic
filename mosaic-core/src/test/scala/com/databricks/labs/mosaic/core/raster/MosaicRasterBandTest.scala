package com.databricks.labs.mosaic.core.raster

import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class MosaicRasterBandTest extends AnyFunSuite with MockFactory {

  abstract class TestMosaicRasterBand extends MosaicRasterBand {
    override final def values: Array[Double] = super.values

    override final def maskValues: Array[Double] = super.maskValues
  }

  test("MosaicRasterBand") {
    val mockMosaicRasterBand = mock[TestMosaicRasterBand]
    mockMosaicRasterBand.xSize _ expects() returning 1 anyNumberOfTimes()
    mockMosaicRasterBand.ySize _ expects() returning 1 anyNumberOfTimes()
    (mockMosaicRasterBand.values(_: Int, _: Int, _: Int, _: Int)) expects(0, 0, 1, 1) returning Array(1.0) anyNumberOfTimes()
    (mockMosaicRasterBand.maskValues(_: Int, _: Int, _: Int, _: Int)) expects(0, 0, 1, 1) returning Array(1.0) anyNumberOfTimes()

    mockMosaicRasterBand.values shouldBe Array(1.0)
    mockMosaicRasterBand.maskValues shouldBe Array(1.0)
  }

}
