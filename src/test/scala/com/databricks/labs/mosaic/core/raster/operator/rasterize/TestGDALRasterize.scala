package com.databricks.labs.mosaic.core.raster.operator.rasterize

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class TestGDALRasterize extends AnyFunSuite {
    test("TestGDALRasterize: should rasterize a point geometry using Z") {
        val point = "POINT Z (1 2 1337)"
        val pointGeom = GeometryAPI.apply("JTS").geometry(point, "WKT")
        pointGeom.setSpatialReference(4326)

        val origin = "POINT (0 2)" // top left-hand corner of north-up image
        val originGeom = GeometryAPI.apply("JTS").geometry(origin, "WKT").asInstanceOf[MosaicPoint]
        originGeom.setSpatialReference(4326)

        val raster = GDALRasterize.executeRasterize(Seq(pointGeom), None, originGeom, 3, 3, 1.0, -1.0)

        val result = raster.getBand(1).values
        result shouldBe Seq(-9999, 1337, -9999, -9999, -9999, -9999, -9999, -9999, -9999)
    }
}
