package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.expressions.accessors.{RST_Avg, RST_BandMetaData, RST_BoundingBox, RST_Format, RST_GeoReference, RST_GetNoData, RST_GetSubdataset, RST_Height, RST_Max, RST_Median, RST_MemSize, RST_MetaData, RST_Min, RST_NumBands, RST_PixelCount, RST_PixelHeight, RST_PixelWidth, RST_Rotation, RST_SRID, RST_ScaleX, RST_ScaleY, RST_SkewX, RST_SkewY, RST_Subdatasets, RST_Summary, RST_Type, RST_UpperLeftX, RST_UpperLeftY, RST_Width}
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.{Files, Paths}

class RST_AccessorsExecuteTest extends AnyFunSuite with BeforeAndAfterAll {

    var ds: Dataset = _

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp")
        gdal.AllRegister()
        val tifPath = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").toString.replace("file:/", "/")
        ds = gdal.Open(tifPath)
    }

    override def afterAll(): Unit = {
        ds.delete()
    }

    test("RST_Avg should return the average px value of the raster") {
        val expected = ds.GetRasterBand(1).AsMDArray().GetStatistics().getMean
        val avg = RST_Avg.execute(ds)
        avg.foreach(a => a shouldBe expected)
    }

    test("RST_BandMetaData should return a map of metadata") {
        val b = ds.GetRasterBand(1)
        val mtd = RST_BandMetaData.execute(b)
        mtd should not be null
        b.delete()
    }

    test("RST_BoundingBox should return a non empty polygon") {
        val bbox = RST_BoundingBox.execute(ds)
        bbox should not be null
        bbox.isEmpty shouldBe false
    }

    test("RST_Format should return short name of the driver") {
        val format = RST_Format.execute(ds)
        format shouldBe a[String]
        format shouldBe "GTiff"
    }

    test("RST_GeoReference should return 6 elements of the raster affine") {
        val gt = RST_GeoReference.execute(ds)
        gt.size shouldBe 6
        gt shouldBe Map(
          "scaleY" -> -463.3127165274999,
          "skewX" -> 0.0,
          "skewY" -> 0.0,
          "upperLeftY" -> 2223901.039333,
          "upperLeftX" -> -8895604.157333,
          "scaleX" -> 463.31271652749973
        )
    }

    test("RST_GetNoData should return designated no data values") {
        val noData = RST_GetNoData.execute(ds)
        noData.isEmpty shouldBe false
        noData should contain theSameElementsAs Array(32767.0)
    }

    test("RST_GetSubdataset should return a subdaset of the raster") {
        val netcdfPath = this.getClass
            .getResource(
              "/binary/netcdf-CMIP5/prAdjust_day_HadGEM2-CC_SMHI-DBSrev930-GFD-1981-2010-postproc_rcp45_r1i1p1_20201201-20201231.nc"
            )
            .toString
            .replace("file:/", "/")
        val netcdf = gdal.Open(netcdfPath)
        val sds = RST_GetSubdataset.execute(netcdf, "prAdjust")
        sds should not be null
        sds.GetRasterBand(1).AsMDArray().GetStatistics().getValid_count should not be 0
        netcdf.delete()
    }

    test("RST_Height should return height of the raster") {
        val height = RST_Height.execute(ds)
        height should be > 0
        height shouldBe ds.GetRasterYSize()
    }

    test("RST_Max should return the max value of the raster per band") {
        val max = RST_Max.execute(ds)
        max.head shouldBe ds.GetRasterBand(1).AsMDArray().GetStatistics().getMax
    }

    test("RST_Median should return approximated median of a single band") {
        val median = RST_Median.execute(ds, Map.empty)
        val buf = Array.ofDim[Double](ds.GetRasterXSize() * ds.GetRasterYSize())
        ds.GetRasterBand(1).ReadRaster(0, 0, ds.GetRasterXSize(), ds.GetRasterYSize(), buf)
        val noData = RST_GetNoData.execute(ds)
        val sorted = buf.filterNot(_ == noData.head).sortBy(identity)
        val expected = sorted(sorted.length / 2)
        val epsilon = expected / 100.0
        median.head shouldBe expected +- epsilon
    }

    test("RST_MemSize should return approximated memory storage size") {
        val memSize = RST_MemSize.execute(ds)
        val location = ds.GetDescription()
        memSize should not be 0
        val expected = Files.size(Paths.get(location))
        memSize shouldBe expected
        val cpy = ds.GetDriver().CreateCopy("/vsimem/cpy.tif", ds, 1)
        val expected2 = gdal.GetMemFileBuffer("/vsimem/cpy.tif")
        val memSize2 = RST_MemSize.execute(cpy)
        memSize2 shouldBe expected2.length
        cpy.delete()
        gdal.Unlink("/vsimem/cpy.tif")
    }

    test("RST_MetaData should return a map of metadata") {
        val mtd = RST_MetaData.execute(ds)
        mtd should not be null
        mtd should contain key "AREA_OR_POINT"
    }

    test("RST_Min should return the min value of the raster per band") {
        val min = RST_Min.execute(ds)
        min.head shouldBe ds.GetRasterBand(1).AsMDArray().GetStatistics().getMin
    }

    test("RST_NumBands should return the number of bands of the raster") {
        val bandCount = RST_NumBands.execute(ds)
        bandCount should be > 0
        bandCount shouldBe ds.GetRasterCount()
    }

    test("RST_PixelCount should return the number of pixels of the raster") {
        val pxCount = RST_PixelCount.execute(ds).head
        pxCount should be > 0L
        val buf = Array.ofDim[Double](ds.GetRasterXSize() * ds.GetRasterYSize())
        ds.GetRasterBand(1).ReadRaster(0, 0, ds.GetRasterXSize(), ds.GetRasterYSize(), buf)
        val noData = RST_GetNoData.execute(ds)
        val validPxCount = buf.count(_ != noData.head)
        pxCount shouldBe validPxCount
    }

    test("RST_PixelHeight should return the pixel height of the raster") {
        val pxHeight = RST_PixelHeight.execute(ds)
        pxHeight should not be 0.0
        pxHeight shouldBe math.abs(ds.GetGeoTransform()(5))
    }

    test("RST_PixelWidth should return the pixel width of the raster") {
        val pxWidth = RST_PixelWidth.execute(ds)
        pxWidth should not be 0.0
        pxWidth shouldBe ds.GetGeoTransform()(1)
    }

    test("RST_Rotation should return the rotation of the raster" ) {
        val rotation = RST_Rotation.execute(ds)
        rotation shouldBe 0.0
        val gt = ds.GetGeoTransform()
        gt(2) shouldBe 0.0
        gt(4) shouldBe 0.0
    }

    test("RST_ScaleX should return the scale x of the raster") {
        val scaleX = RST_ScaleX.execute(ds)
        scaleX should not be 0.0
        scaleX shouldBe ds.GetGeoTransform()(1)
    }

    test("RST_ScaleY should return the scale y of the raster") {
        val scaleY = RST_ScaleY.execute(ds)
        scaleY should not be 0.0
        scaleY shouldBe ds.GetGeoTransform()(5)
    }

    test("RST_SkewX should return the skew x of the raster") {
        val skewX = RST_SkewX.execute(ds)
        skewX shouldBe 0.0
        val gt = ds.GetGeoTransform()
        gt(2) shouldBe 0.0
    }

    test("RST_SkewY should return the skew y of the raster") {
        val skewY = RST_SkewY.execute(ds)
        skewY shouldBe 0.0
        val gt = ds.GetGeoTransform()
        gt(4) shouldBe 0.0
    }

    test("RST_SRID should return the spatial reference ID of the raster") {
        val srid = RST_SRID.execute(ds)
        srid shouldBe 9122
    }

    test("RST_Subdatasets should return a map of subdatasets" ) {
        val netcdfPath = this.getClass
            .getResource(
              "/binary/netcdf-CMIP5/prAdjust_day_HadGEM2-CC_SMHI-DBSrev930-GFD-1981-2010-postproc_rcp45_r1i1p1_20201201-20201231.nc"
            )
            .toString
            .replace("file:/", "/")
        val netcdf = gdal.Open(netcdfPath)
        val sdsMap = RST_Subdatasets.execute(netcdf)
        sdsMap should not be null
        sdsMap should contain key "SUBDATASET_1_NAME"
        netcdf.delete()
    }

    test("RST_Summary should return a summary of the raster") {
        val summary = RST_Summary.execute(ds)
        summary should not be null
        summary.contains("driverShortName") shouldBe true
        summary.contains("size") shouldBe true
        summary.contains("coordinateSystem") shouldBe true
    }

    test("RST_Type should return the data type of the raster") {
        val dtype = RST_Type.execute(ds).head
        dtype shouldBe "Int16"
    }

    test("RST_UpperLeftX should return the upper left x coordinate of the raster") {
        val ulx = RST_UpperLeftX.execute(ds)
        ulx shouldBe ds.GetGeoTransform()(0)
    }

    test("RST_UpperLeftY should return the upper left y coordinate of the raster") {
        val uly = RST_UpperLeftY.execute(ds)
        uly shouldBe ds.GetGeoTransform()(3)
    }

    test("RST_Width should return width of the raster") {
        val width = RST_Width.execute(ds)
        width should be > 0
        width shouldBe ds.GetRasterXSize()
    }


}
