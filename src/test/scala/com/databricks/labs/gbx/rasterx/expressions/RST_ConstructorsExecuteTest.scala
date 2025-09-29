package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.expressions.constructor.RST_FromBands
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class RST_ConstructorsExecuteTest extends AnyFunSuite with BeforeAndAfterAll {

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

    test("RST_FromBands should create a raster from bands") {
        val (outDs, outMtd) = RST_FromBands.execute(Seq((1L, ds, Map("key" -> "value")), (2L, ds, Map("key" -> "value"))))
        outDs should not be null
        outMtd should not be null
        outMtd("path") should not be ""
        val path = outMtd("path")
        outDs.delete()
        gdal.Unlink(path)
    }

}
