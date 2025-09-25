package com.dblabs.gbx.rasterx.expressions

import com.dblabs.gbx.rasterx.expressions.grid._
import com.dblabs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class RST_GridExecuteTest extends AnyFunSuite with BeforeAndAfterAll {

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

    test("RST_H3_RasterToGridAvg should produce average cells") {
        val result = RST_H3_RasterToGridAvg.execute(ds, 2)
        result.length shouldBe 1
        result(0).length should be > 0
        val sample = result(0).take(5)
        sample.foreach { case (cellID, measure) =>
            cellID should be > 0L
            measure should be >= 0.0
        }
    }

    test("RST_H3_RasterToGridCount should produce count cells") {
        val result = RST_H3_RasterToGridCount.execute(ds, 2)
        result.length shouldBe 1
        result(0).length should be > 0
        val sample = result(0).take(5)
        sample.foreach { case (cellID, measure) =>
            cellID should be > 0L
            measure should be >= 0
        }
    }

    test("RST_H3_RasterToGridMax should produce max cells") {
        val result = RST_H3_RasterToGridMax.execute(ds, 2)
        result.length shouldBe 1
        result(0).length should be > 0
        val sample = result(0).take(5)
        sample.foreach { case (cellID, measure) =>
            cellID should be > 0L
            measure should be >= 0.0
        }
    }

    test("RST_H3_RasterToGridMin should produce min cells") {
        val result = RST_H3_RasterToGridMin.execute(ds, 2)
        result.length shouldBe 1
        result(0).length should be > 0
        val sample = result(0).take(5)
        sample.foreach { case (cellID, measure) =>
            cellID should be > 0L
            measure should be >= 0.0
        }
    }

    test("RST_H3_RasterToGridMedian should produce median cells") {
        val result = RST_H3_RasterToGridMedian.execute(ds, 2)
        result.length shouldBe 1
        result(0).length should be > 0
        val sample = result(0).take(5)
        sample.foreach { case (cellID, measure) =>
            cellID should be > 0L
            measure should be >= 0.0
        }
    }

}
