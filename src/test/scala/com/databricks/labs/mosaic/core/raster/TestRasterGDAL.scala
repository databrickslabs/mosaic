package com.databricks.labs.mosaic.core.raster

import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.gdal.MosaicGDAL
import com.databricks.labs.mosaic.test.mocks.filePath
import org.apache.spark.sql.test.SharedSparkSessionGDAL
import org.scalatest.matchers.should.Matchers._
import org.gdal.gdal.{gdal => gdalJNI}
import org.gdal.gdalconst

import scala.sys.process._
import scala.util.Try

class TestRasterGDAL extends SharedSparkSessionGDAL {

    test("Verify that GDAL is enabled.") {
        assume(System.getProperty("os.name") == "Linux")

        val checkCmd = "gdalinfo --version"
        val resultDriver = Try(checkCmd.!!).getOrElse("")
        resultDriver should not be ""
        resultDriver should include("GDAL")

        val sc = spark.sparkContext
        val numExecutors = sc.getExecutorMemoryStatus.size - 1
        val resultExecutors = Try(
          sc.parallelize(1 to numExecutors)
              .pipe(checkCmd)
              .collect
        ).getOrElse(Array[String]())
        resultExecutors.length should not be 0
        resultExecutors.foreach(s => s should include("GDAL"))
    }

    test("Read raster metadata from GeoTIFF file.") {
        assume(System.getProperty("os.name") == "Linux")
        
        val createInfo = Map(
          "path" -> filePath("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"),
          "parentPath" -> filePath("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
        )
        val testRaster = MosaicRasterGDAL.readRaster(createInfo)
        testRaster.xSize shouldBe 2400
        testRaster.ySize shouldBe 2400
        testRaster.numBands shouldBe 1
        testRaster.proj4String shouldBe "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +R=6371007.181 +units=m +no_defs"
        testRaster.SRID shouldBe 0
        testRaster.extent shouldBe Seq(-8895604.157333, 1111950.519667, -7783653.637667, 2223901.039333)
        testRaster.getRaster.GetProjection()
        noException should be thrownBy testRaster.getSpatialReference
        an[Exception] should be thrownBy testRaster.getBand(-1)
        an[Exception] should be thrownBy testRaster.getBand(Int.MaxValue)

        testRaster.getRaster.delete()
    }

    test("Read raster metadata from a GRIdded Binary file.") {
        assume(System.getProperty("os.name") == "Linux")

        val createInfo = Map(
          "path" -> filePath("/binary/grib-cams/adaptor.mars.internal-1650626995.380916-11651-14-ca8e7236-16ca-4e11-919d-bdbd5a51da35.grb"),
          "parentPath" -> filePath("/binary/grib-cams/adaptor.mars.internal-1650626995.380916-11651-14-ca8e7236-16ca-4e11-919d-bdbd5a51da35.grb")
        )
        val testRaster = MosaicRasterGDAL.readRaster(createInfo)
        testRaster.xSize shouldBe 14
        testRaster.ySize shouldBe 14
        testRaster.numBands shouldBe 14
        testRaster.proj4String shouldBe "+proj=longlat +R=6371229 +no_defs"
        testRaster.SRID shouldBe 0
        testRaster.extent shouldBe Seq(-0.375, -0.375, 10.125, 10.125)

        testRaster.getRaster.delete()
    }

    test("Read raster metadata from a NetCDF file.") {
        assume(System.getProperty("os.name") == "Linux")
        
        val createInfo = Map(
          "path" -> filePath("/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220101.nc"),
          "parentPath" -> filePath("/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220101.nc")
        )
        val superRaster = MosaicRasterGDAL.readRaster(createInfo)
        val subdatasetPath = superRaster.subdatasets("bleaching_alert_area")

        val sdCreateInfo = Map(
          "path" -> subdatasetPath,
          "parentPath" -> subdatasetPath
        )
        val testRaster = MosaicRasterGDAL.readRaster(sdCreateInfo)

        testRaster.xSize shouldBe 7200
        testRaster.ySize shouldBe 3600
        testRaster.numBands shouldBe 1
        testRaster.proj4String shouldBe "+proj=longlat +a=6378137 +rf=298.2572 +no_defs"
        testRaster.SRID shouldBe 0
        testRaster.extent shouldBe Seq(-180.00000610436345, -89.99999847369712, 180.00000610436345, 89.99999847369712)

        testRaster.getRaster.delete()
        superRaster.getRaster.delete()
    }

    test("Raster pixel and extent sizes are correct.") {
        assume(System.getProperty("os.name") == "Linux")

        val createInfo = Map(
          "path" -> filePath("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"),
          "parentPath" -> filePath("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
        )
        val testRaster = MosaicRasterGDAL.readRaster(createInfo)

        testRaster.pixelXSize - 463.312716527 < 0.0000001 shouldBe true
        testRaster.pixelYSize - -463.312716527 < 0.0000001 shouldBe true
        testRaster.pixelDiagSize - 655.22312733 < 0.0000001 shouldBe true

        testRaster.diagSize - 3394.1125496954 < 0.0000001 shouldBe true
        testRaster.originX - -8895604.157333 < 0.0000001 shouldBe true
        testRaster.originY - 2223901.039333 < 0.0000001 shouldBe true
        testRaster.xMax - -7783653.637667 < 0.0000001 shouldBe true
        testRaster.yMax - 1111950.519667 < 0.0000001 shouldBe true
        testRaster.xMin - -8895604.157333 < 0.0000001 shouldBe true
        testRaster.yMin - 2223901.039333 < 0.0000001 shouldBe true

        testRaster.getRaster.delete()
    }

    test("Raster filter operations are correct.") {
        assume(System.getProperty("os.name") == "Linux")

        gdalJNI.AllRegister()

        MosaicGDAL.setBlockSize(30)

        val ds = gdalJNI.GetDriverByName("GTiff").Create("/tmp/mosaic_tmp/test.tif", 50, 50, 1, gdalconst.gdalconstConstants.GDT_Float32)

        val values = 0 until 50 * 50
        ds.GetRasterBand(1).WriteRaster(0, 0, 50, 50, values.toArray)
        ds.FlushCache()

        val createInfo = Map(
          "path" -> "",
          "parentPath" -> "",
          "driver" -> "GTiff"
        )
        var result = MosaicRasterGDAL(ds, createInfo, -1).filter(5, "avg").flushCache()

        var resultValues = result.getBand(1).values

        var inputMatrix = values.toArray.grouped(50).toArray
        var resultMatrix = resultValues.grouped(50).toArray

        // first block
        resultMatrix(10)(11) shouldBe (
          inputMatrix(8)(9) + inputMatrix(8)(10) + inputMatrix(8)(11) + inputMatrix(8)(12) + inputMatrix(8)(13) +
              inputMatrix(9)(9) + inputMatrix(9)(10) + inputMatrix(9)(11) + inputMatrix(9)(12) + inputMatrix(9)(13) +
              inputMatrix(10)(9) + inputMatrix(10)(10) + inputMatrix(10)(11) + inputMatrix(10)(12) + inputMatrix(10)(13) +
              inputMatrix(11)(9) + inputMatrix(11)(10) + inputMatrix(11)(11) + inputMatrix(11)(12) + inputMatrix(11)(13) +
              inputMatrix(12)(9) + inputMatrix(12)(10) + inputMatrix(12)(11) + inputMatrix(12)(12) + inputMatrix(12)(13)
        ).toDouble / 25.0

        // block overlap
        resultMatrix(30)(32) shouldBe (
          inputMatrix(28)(30) + inputMatrix(28)(31) + inputMatrix(28)(32) + inputMatrix(28)(33) + inputMatrix(28)(34) +
              inputMatrix(29)(30) + inputMatrix(29)(31) + inputMatrix(29)(32) + inputMatrix(29)(33) + inputMatrix(29)(34) +
              inputMatrix(30)(30) + inputMatrix(30)(31) + inputMatrix(30)(32) + inputMatrix(30)(33) + inputMatrix(30)(34) +
              inputMatrix(31)(30) + inputMatrix(31)(31) + inputMatrix(31)(32) + inputMatrix(31)(33) + inputMatrix(31)(34) +
              inputMatrix(32)(30) + inputMatrix(32)(31) + inputMatrix(32)(32) + inputMatrix(32)(33) + inputMatrix(32)(34)
        ).toDouble / 25.0

        // mode

        result = MosaicRasterGDAL(ds, createInfo, -1).filter(5, "mode").flushCache()

        resultValues = result.getBand(1).values

        inputMatrix = values.toArray.grouped(50).toArray
        resultMatrix = resultValues.grouped(50).toArray

        // first block

        resultMatrix(10)(11) shouldBe Seq(
          inputMatrix(8)(9),
          inputMatrix(8)(10),
          inputMatrix(8)(11),
          inputMatrix(8)(12),
          inputMatrix(8)(13),
          inputMatrix(9)(9),
          inputMatrix(9)(10),
          inputMatrix(9)(11),
          inputMatrix(9)(12),
          inputMatrix(9)(13),
          inputMatrix(10)(9),
          inputMatrix(10)(10),
          inputMatrix(10)(11),
          inputMatrix(10)(12),
          inputMatrix(10)(13),
          inputMatrix(11)(9),
          inputMatrix(11)(10),
          inputMatrix(11)(11),
          inputMatrix(11)(12),
          inputMatrix(11)(13),
          inputMatrix(12)(9),
          inputMatrix(12)(10),
          inputMatrix(12)(11),
          inputMatrix(12)(12),
          inputMatrix(12)(13)
        ).groupBy(identity).maxBy(_._2.size)._1.toDouble

        // corner

        resultMatrix(49)(49) shouldBe Seq(
          inputMatrix(47)(47),
          inputMatrix(47)(48),
          inputMatrix(47)(49),
          inputMatrix(48)(47),
          inputMatrix(48)(48),
          inputMatrix(48)(49),
          inputMatrix(49)(47),
          inputMatrix(49)(48),
          inputMatrix(49)(49)
        ).groupBy(identity).maxBy(_._2.size)._1.toDouble

        // median

        result = MosaicRasterGDAL(ds, createInfo, -1).filter(5, "median").flushCache()

        resultValues = result.getBand(1).values

        inputMatrix = values.toArray.grouped(50).toArray
        resultMatrix = resultValues.grouped(50).toArray

        // first block

        resultMatrix(10)(11) shouldBe Seq(
          inputMatrix(8)(9),
          inputMatrix(8)(10),
          inputMatrix(8)(11),
          inputMatrix(8)(12),
          inputMatrix(8)(13),
          inputMatrix(9)(9),
          inputMatrix(9)(10),
          inputMatrix(9)(11),
          inputMatrix(9)(12),
          inputMatrix(9)(13),
          inputMatrix(10)(9),
          inputMatrix(10)(10),
          inputMatrix(10)(11),
          inputMatrix(10)(12),
          inputMatrix(10)(13),
          inputMatrix(11)(9),
          inputMatrix(11)(10),
          inputMatrix(11)(11),
          inputMatrix(11)(12),
          inputMatrix(11)(13),
          inputMatrix(12)(9),
          inputMatrix(12)(10),
          inputMatrix(12)(11),
          inputMatrix(12)(12),
          inputMatrix(12)(13)
        ).sorted.apply(12).toDouble

        // min filter

        result = MosaicRasterGDAL(ds, createInfo, -1).filter(5, "min").flushCache()

        resultValues = result.getBand(1).values

        inputMatrix = values.toArray.grouped(50).toArray
        resultMatrix = resultValues.grouped(50).toArray

        // first block

        resultMatrix(10)(11) shouldBe Seq(
          inputMatrix(8)(9),
          inputMatrix(8)(10),
          inputMatrix(8)(11),
          inputMatrix(8)(12),
          inputMatrix(8)(13),
          inputMatrix(9)(9),
          inputMatrix(9)(10),
          inputMatrix(9)(11),
          inputMatrix(9)(12),
          inputMatrix(9)(13),
          inputMatrix(10)(9),
          inputMatrix(10)(10),
          inputMatrix(10)(11),
          inputMatrix(10)(12),
          inputMatrix(10)(13),
          inputMatrix(11)(9),
          inputMatrix(11)(10),
          inputMatrix(11)(11),
          inputMatrix(11)(12),
          inputMatrix(11)(13),
          inputMatrix(12)(9),
          inputMatrix(12)(10),
          inputMatrix(12)(11),
          inputMatrix(12)(12),
          inputMatrix(12)(13)
        ).min.toDouble

        // max filter

        result = MosaicRasterGDAL(ds, createInfo, -1).filter(5, "max").flushCache()

        resultValues = result.getBand(1).values

        inputMatrix = values.toArray.grouped(50).toArray
        resultMatrix = resultValues.grouped(50).toArray

        // first block

        resultMatrix(10)(11) shouldBe Seq(
          inputMatrix(8)(9),
          inputMatrix(8)(10),
          inputMatrix(8)(11),
          inputMatrix(8)(12),
          inputMatrix(8)(13),
          inputMatrix(9)(9),
          inputMatrix(9)(10),
          inputMatrix(9)(11),
          inputMatrix(9)(12),
          inputMatrix(9)(13),
          inputMatrix(10)(9),
          inputMatrix(10)(10),
          inputMatrix(10)(11),
          inputMatrix(10)(12),
          inputMatrix(10)(13),
          inputMatrix(11)(9),
          inputMatrix(11)(10),
          inputMatrix(11)(11),
          inputMatrix(11)(12),
          inputMatrix(11)(13),
          inputMatrix(12)(9),
          inputMatrix(12)(10),
          inputMatrix(12)(11),
          inputMatrix(12)(12),
          inputMatrix(12)(13)
        ).max.toDouble

    }

}
