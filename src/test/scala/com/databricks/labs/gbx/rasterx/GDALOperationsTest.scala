package com.databricks.labs.gbx.rasterx

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.rasterx.operations.RasterAccessors
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class GDALOperationsTest extends AnyFunSuite with BeforeAndAfterAll {

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

    test("RasterAccessors should run isEmpty on rasters with subdatasets") {
        val dummy = gdal.GetDriverByName("GTiff").Create("/vsimem/dummy.tif", 10, 10, 1)
        // create a subdataset
        dummy.SetMetadataItem("SUBDATASET_1_NAME", "/vsimem/dummy.tif:sub1")
        dummy.SetMetadataItem("SUBDATASET_1_DESC", "Dummy subdataset 1")
        // add bands to subdataset
        val subDs = gdal.GetDriverByName("MEM").Create("/vsimem/dummy.tif:sub1", 10, 10, 1)
        subDs.GetRasterBand(1).Fill(100)
        subDs.FlushCache()
        subDs.delete()
        dummy.FlushCache()
        RasterAccessors.isEmpty(dummy) shouldBe false
        dummy.delete()
    }




}
