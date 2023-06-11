package com.databricks.labs.mosaic.core.raster

import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class MosaicRasterTest extends AnyFunSuite with MockFactory {

  abstract class TestMosaicRaster extends MosaicRaster("path", 1234) {
    override final def getMemSize: Long = super.getMemSize
  }

  test("MosaicRaster") {
    val mockMosaicRaster = mock[TestMosaicRaster]
    mockMosaicRaster.getMemSize shouldBe 1234
  }

}
