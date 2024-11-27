package com.databricks.labs.mosaic.core.raster

import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.test.mocks.filePath
import org.apache.spark.sql.test.SharedSparkSessionGDAL
import org.scalatest.matchers.should.Matchers._

class TestRasterBandGDAL extends SharedSparkSessionGDAL {

    test("Read band metadata and pixel data from GeoTIFF file.") {
        assume(System.getProperty("os.name") == "Linux")

        val createInfo = Map(
          "path" -> filePath("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"),
          "parentPath" -> filePath("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
        )
        val testRaster = MosaicRasterGDAL.readRaster(createInfo)
        val testBand = testRaster.getBand(1)
        testBand.getBand
        testBand.index shouldBe 1
        testBand.units shouldBe ""
        testBand.description shouldBe "Nadir_Reflectance_Band1"
        testBand.dataType shouldBe 3
        testBand.xSize shouldBe 2400
        testBand.ySize shouldBe 2400
        testBand.noDataValue shouldBe 32767.0
        (testBand.minPixelValue, testBand.maxPixelValue) shouldBe (0d, 6940d)
        (testBand.pixelValueScale, testBand.pixelValueOffset) shouldBe (1.0e-4, 0d)
        testBand.pixelValueToUnitValue(100) shouldBe 100e-4

        val testValues = testBand.values(1000, 1000, 100, 50)
        testValues.length shouldBe 5000

        testRaster.getRaster.delete()
    }

    test("Read band metadata and pixel data from a GRIdded Binary file.") {
        assume(System.getProperty("os.name") == "Linux")

        val createInfo = Map(
            "path" -> filePath("/binary/grib-cams/adaptor.mars.internal-1650626995.380916-11651-14-ca8e7236-16ca-4e11-919d-bdbd5a51da35.grb"),
            "parentPath" -> filePath("/binary/grib-cams/adaptor.mars.internal-1650626995.380916-11651-14-ca8e7236-16ca-4e11-919d-bdbd5a51da35.grb")
        )
        val testRaster = MosaicRasterGDAL.readRaster(createInfo)
        val testBand = testRaster.getBand(1)
        testBand.description shouldBe "1[-] HYBL=\"Hybrid level\""
        testBand.dataType shouldBe 7
        (testBand.minPixelValue, testBand.maxPixelValue) shouldBe (1.1368277910150937e-6, 1.2002082030448946e-6)
        (testBand.pixelValueScale, testBand.pixelValueOffset) shouldBe (0d, 0d)

        val testValues = testBand.values(1, 1, 4, 5)
        testValues.length shouldBe 20

        testRaster.getRaster.delete()
    }

    test("Read band metadata and pixel data from a NetCDF file.") {
        assume(System.getProperty("os.name") == "Linux")

        val createInfo = Map(
            "path" -> filePath("/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220101.nc"),
            "parentPath" -> filePath("/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220101.nc")
        )
        val superRaster = MosaicRasterGDAL.readRaster(createInfo)
        val subdatasetPath = superRaster.subdatasets("bleaching_alert_area")
        val sdCreate  = Map(
            "path" -> subdatasetPath,
            "parentPath" -> subdatasetPath
        )
        val testRaster = MosaicRasterGDAL.readRaster(sdCreate)

        val testBand = testRaster.getBand(1)
        testBand.dataType shouldBe 1
        (testBand.minPixelValue, testBand.maxPixelValue) shouldBe (0d, 4d)
        (testBand.pixelValueScale, testBand.pixelValueOffset) shouldBe (1d, 0d)

        val testValues = testBand.values(5000, 1000, 100, 10)
        noException should be thrownBy testBand.values
        testValues.length shouldBe 1000

        testRaster.getRaster.delete()
        superRaster.getRaster.delete()
    }

}
