package com.databricks.labs.mosaic.core.raster

import com.databricks.labs.mosaic.{ESRI, H3, GDAL}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{mocks, SparkSuite}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class TestRasterBandGDAL extends AnyFlatSpec with SparkSuite {

    "MosaicRasterBandGDAL" should "read band metadata and pixel data from a GeoTIFF file." in {
        assume(System.getProperty("os.name") == "Linux")
        val mc = MosaicContext.build(H3, ESRI, GDAL)
        mc.enableGDAL(spark)

        val testRaster = MosaicRasterGDAL.fromBytes(mocks.geotiffBytes)

        val testBand = testRaster.getBand(1)
        testBand.description shouldBe "Nadir_Reflectance_Band1"
        testBand.dataType shouldBe 3
        (testBand.minPixelValue, testBand.maxPixelValue) shouldBe (0d, 6940d)
        (testBand.pixelValueScale, testBand.pixelValueOffset) shouldBe (1.0e-4, 0d)

        val testValues = testBand.values(1000, 1000, 100, 50)
        testValues.length shouldBe 50
        testValues.head.length shouldBe 100
    }

    "MosaicRasterBandGDAL" should "read band metadata and pixel data from a GRIdded Binary file." in {
        assume(System.getProperty("os.name") == "Linux")
        val mc = MosaicContext.build(H3, ESRI, GDAL)
        mc.enableGDAL(spark)

        val testRaster = MosaicRasterGDAL.fromBytes(mocks.gribBytes)

        val testBand = testRaster.getBand(1)
        testBand.description shouldBe "1[-] HYBL=\"Hybrid level\""
        testBand.dataType shouldBe 7
        (testBand.minPixelValue, testBand.maxPixelValue) shouldBe (1.1368277910150937e-6, 1.2002082030448946e-6)
        (testBand.pixelValueScale, testBand.pixelValueOffset) shouldBe (1.0, 0d)

        val testValues = testBand.values(1, 1, 4, 5)
        testValues.length shouldBe 5
        testValues.head.length shouldBe 4
    }

    "MosaicRasterBandGDAL" should "read band metadata and pixel data from a NetCDF file." in {
        assume(System.getProperty("os.name") == "Linux")
        val mc = MosaicContext.build(H3, ESRI, GDAL)
        mc.enableGDAL(spark)

        val superRaster = MosaicRasterGDAL.fromBytes(mocks.netcdfBytes)
        val subdatasetPath = superRaster.subdatasets.filterKeys(_.contains("bleaching_alert_area")).head._1

        println(subdatasetPath)

        val testRaster = MosaicRasterGDAL.fromBytes(mocks.netcdfBytes, subdatasetPath)

        val testBand = testRaster.getBand(1)
        testBand.dataType shouldBe 1
        (testBand.minPixelValue, testBand.maxPixelValue) shouldBe (0d, 251d)
        (testBand.pixelValueScale, testBand.pixelValueOffset) shouldBe (1d, 0d)

        val testValues = testBand.values(5000, 1000, 100, 10)
        testValues.length shouldBe 10
        testValues.head.length shouldBe 100
    }

}
