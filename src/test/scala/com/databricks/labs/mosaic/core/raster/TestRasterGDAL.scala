package com.databricks.labs.mosaic.core.raster

import com.databricks.labs.mosaic.core.raster.api.RasterAPI
import com.databricks.labs.mosaic.test.mocks
import com.databricks.labs.mosaic.GDAL
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.test.SharedSparkSessionGDAL
import org.scalatest.matchers.should.Matchers._

import scala.sys.process._
import scala.util.Try

class TestRasterGDAL extends SharedSparkSessionGDAL {

    test("Verify that GDAL is enabled.") {
        val checkCmd = "gdalinfo --version"
        val resultDriver = Try(checkCmd.!!).getOrElse("")
        resultDriver should not be ""
        resultDriver should include("GDAL")

        val sc = spark.sparkContext
        val numExecutors = sc.getExecutorMemoryStatus.size - 1
        val resultExecutors = Try(
            sc.parallelize(1 to numExecutors)
                .pipe(checkCmd).collect).getOrElse(Array[String]()
        )
        resultExecutors.length should not be 0
        resultExecutors.foreach(s => s should include("GDAL"))
    }

    test("Read raster metadata from GeoTIFF file.") {
        val testRaster = MosaicRasterGDAL.fromBytes(mocks.geotiffBytes)
        testRaster.xSize shouldBe 2400
        testRaster.ySize shouldBe 2400
        testRaster.numBands shouldBe 1
        testRaster.proj4String shouldBe "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +R=6371007.181 +units=m +no_defs"
        testRaster.SRID shouldBe 0
        testRaster.extent shouldBe Seq(-8895604.157333, 1111950.519667, -7783653.637667, 2223901.039333)
        testRaster.getRaster.GetProjection()
        noException should be thrownBy testRaster.asInstanceOf[MosaicRasterGDAL].spatialRef
        noException should be thrownBy testRaster.geoTransform(1, 1)
        testRaster.cleanUp()
    }

    test("Read raster metadata from a GRIdded Binary file.") {
        val testRaster = MosaicRasterGDAL.fromBytes(mocks.gribBytes)
        testRaster.xSize shouldBe 14
        testRaster.ySize shouldBe 14
        testRaster.numBands shouldBe 14
        testRaster.proj4String shouldBe "+proj=longlat +R=6371229 +no_defs"
        testRaster.SRID shouldBe 0
        testRaster.extent shouldBe Seq(-0.375, -0.375, 10.125, 10.125)
        testRaster.cleanUp()
    }

    test("Read raster metadata from a NetCDF file.") {
        val superRaster = MosaicRasterGDAL.fromBytes(mocks.netcdfBytes)
        val subdatasetPath = superRaster.subdatasets.filterKeys(_.contains("bleaching_alert_area")).head._1

        val testRaster = MosaicRasterGDAL.fromBytes(mocks.netcdfBytes, subdatasetPath)

        testRaster.xSize shouldBe 7200
        testRaster.ySize shouldBe 3600
        testRaster.numBands shouldBe 1
        testRaster.proj4String shouldBe "" // gdal cannot correctly interpret NetCDF CRS
        testRaster.SRID shouldBe 0
        testRaster.extent shouldBe Seq(0d, 3600d, 7200d, 0d) // gdal returns bad extent for NetCDF

        testRaster.cleanUp()
        superRaster.cleanUp()
    }

    test("Raster API") {
        RasterAPI.apply("GDAL") shouldBe GDAL
        RasterAPI.getReader("GDAL") shouldBe MosaicRasterGDAL
        GDAL.name shouldBe "GDAL"
        noException should be thrownBy GDAL.raster(Array.empty[Byte])
        an[Exception] should be thrownBy GDAL.raster(InternalRow.empty)
    }

}
