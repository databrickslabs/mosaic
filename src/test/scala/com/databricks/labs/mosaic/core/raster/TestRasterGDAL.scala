package com.databricks.labs.mosaic.core.raster

import scala.collection.JavaConverters.dictionaryAsScalaMapConverter

import java.nio.file.{Files, Paths}

import com.databricks.labs.mosaic.{ESRI, H3}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkSuite
import org.gdal.gdal.Dataset
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class TestRasterGDAL extends AnyFlatSpec with SparkSuite {

    "MosaicRasterGDAL" should "read raster data and metadata from a GeoTIFF file." in {
        assume(System.getProperty("os.name") == "Linux")
        val mc = MosaicContext.build(H3, ESRI)
        mc.installGDAL(spark)

        val resourcePath = "/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"
        val inFile = getClass.getResource(resourcePath)
        val byteArray = Files.readAllBytes(Paths.get(inFile.getPath))

        val testRaster = MosaicRasterGDAL.fromBytes(byteArray)
        testRaster.xSize shouldBe 2400
        testRaster.ySize shouldBe 2400
        testRaster.numBands shouldBe 1
        testRaster.proj4String shouldBe "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +R=6371007.181 +units=m +no_defs"
        testRaster.SRID shouldBe 0
        testRaster.extent shouldBe Seq(-8895604.157333, 1111950.519667, -7783653.637667, 2223901.039333)

        val testBand = testRaster.getBand(1)
        testBand.description shouldBe "Nadir_Reflectance_Band1"
        testBand.dataType shouldBe 3
        (testBand.minPixelValue, testBand.maxPixelValue) shouldBe (0d, 6940d)
        (testBand.pixelValueScale, testBand.pixelValueOffset) shouldBe (1.0e-4, 0d)

        val testValues = testBand.values(1000, 1000, 100, 50)
        testValues.length shouldBe 50
        testValues.head.length shouldBe 100
    }

    "MosaicRasterGDAL" should "read raster data and metadata from a Gridded Binary file." in {
        assume(System.getProperty("os.name") == "Linux")
        val mc = MosaicContext.build(H3, ESRI)
        mc.installGDAL(spark)

        val resourcePath = "/binary/grib-cams/adaptor.mars.internal-1650626995.380916-11651-14-ca8e7236-16ca-4e11-919d-bdbd5a51da35.grib"
        val inFile = getClass.getResource(resourcePath)
        val byteArray = Files.readAllBytes(Paths.get(inFile.getPath))

        val testRaster = MosaicRasterGDAL.fromBytes(byteArray)
        testRaster.xSize shouldBe 14
        testRaster.ySize shouldBe 14
        testRaster.numBands shouldBe 14
        testRaster.proj4String shouldBe "+proj=longlat +R=6371229 +no_defs"
        testRaster.SRID shouldBe 0
        testRaster.extent shouldBe Seq(-0.375, -0.375, 10.125, 10.125)

        val testBand = testRaster.getBand(1)
        testBand.description shouldBe "1[-] HYBL=\"Hybrid level\""
        testBand.dataType shouldBe 7
        (testBand.minPixelValue, testBand.maxPixelValue) shouldBe (1.1368277910150937e-6, 1.2002082030448946e-6)
        (testBand.pixelValueScale, testBand.pixelValueOffset) shouldBe (1.0, 0d)

        val testValues = testBand.values(1, 1, 4, 5)
        testValues.length shouldBe 5
        testValues.head.length shouldBe 4
    }

    "MosaicRasterGDAL" should "read raster data and metadata from a NetCDF file." in {
        assume(System.getProperty("os.name") == "Linux")
        val mc = MosaicContext.build(H3, ESRI)
        mc.installGDAL(spark)

        val resourcePath = "/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220101.nc"
        val inFile = getClass.getResource(resourcePath)
        val byteArray = Files.readAllBytes(Paths.get(inFile.getPath))

        val superRaster = MosaicRasterGDAL.fromBytes(byteArray)
        val subdatasetPath = superRaster.subdatasets.filterKeys(_.contains("bleaching_alert_area")).head._1

        val testRaster = MosaicRasterGDAL.fromBytes(byteArray, subdatasetPath)

        testRaster.xSize shouldBe 7200
        testRaster.ySize shouldBe 3600
        testRaster.numBands shouldBe 1
        testRaster.proj4String shouldBe ""
        testRaster.SRID shouldBe 0
        testRaster.extent shouldBe Seq(0d, 3600d, 7200d, 0d) // gdal returns bad extent for NetCDF

        val testBand = testRaster.getBand(1)
        testBand.dataType shouldBe 1
        (testBand.minPixelValue, testBand.maxPixelValue) shouldBe (0d, 251d)
        (testBand.pixelValueScale, testBand.pixelValueOffset) shouldBe (1d, 0d)

        val testValues = testBand.values(5000, 1000, 100, 10)
        testValues.length shouldBe 10
        testValues.head.length shouldBe 100
    }

}
