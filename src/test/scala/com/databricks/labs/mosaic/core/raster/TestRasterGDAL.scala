package com.databricks.labs.mosaic.core.raster

import com.databricks.labs.mosaic.{ESRI, GDAL, H3}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{mocks, SparkSuite}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class TestRasterGDAL extends AnyFlatSpec with SparkSuite {

    // expectations gathered using `gdalinfo` utility
    "MosaicRasterGDAL" should "read dataset metadata from a GeoTIFF file." in {
        val mc = MosaicContext.build(H3, ESRI, GDAL)
        mc.enableGDAL(spark)

        val testRaster = MosaicRasterGDAL.fromBytes(mocks.geotiffBytes)
        testRaster.xSize shouldBe 2400
        testRaster.ySize shouldBe 2400
        testRaster.numBands shouldBe 1
        testRaster.proj4String shouldBe "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +R=6371007.181 +units=m +no_defs"
        testRaster.SRID shouldBe 0
        testRaster.extent shouldBe Seq(-8895604.157333, 1111950.519667, -7783653.637667, 2223901.039333)
        testRaster.getRaster.GetProjection()
    }

    "MosaicRasterGDAL" should "read dataset metadata from a Gridded Binary file." in {
        val mc = MosaicContext.build(H3, ESRI, GDAL)
        mc.enableGDAL(spark)

        val testRaster = MosaicRasterGDAL.fromBytes(mocks.gribBytes)
        testRaster.xSize shouldBe 14
        testRaster.ySize shouldBe 14
        testRaster.numBands shouldBe 14
        testRaster.proj4String shouldBe "+proj=longlat +R=6371229 +no_defs"
        testRaster.SRID shouldBe 0
        testRaster.extent shouldBe Seq(-0.375, -0.375, 10.125, 10.125)
    }

    "MosaicRasterGDAL" should "read dataset metadata from a NetCDF file." in {
        val mc = MosaicContext.build(H3, ESRI, GDAL)
        mc.enableGDAL(spark)

        val superRaster = MosaicRasterGDAL.fromBytes(mocks.netcdfBytes)
        val subdatasetPath = superRaster.subdatasets.filterKeys(_.contains("bleaching_alert_area")).head._1

        val testRaster = MosaicRasterGDAL.fromBytes(mocks.netcdfBytes, subdatasetPath)

        testRaster.xSize shouldBe 7200
        testRaster.ySize shouldBe 3600
        testRaster.numBands shouldBe 1
        testRaster.proj4String shouldBe "" // gdal cannot correctly interpret NetCDF CRS
        testRaster.SRID shouldBe 0
        testRaster.extent shouldBe Seq(0d, 3600d, 7200d, 0d) // gdal returns bad extent for NetCDF
    }

}
