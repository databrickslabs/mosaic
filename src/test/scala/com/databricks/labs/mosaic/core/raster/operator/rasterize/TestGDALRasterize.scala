package com.databricks.labs.mosaic.core.raster.operator.rasterize

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class TestGDALRasterize extends AnyFunSuite {

    // https://oeis.org/A341198
    val points: Seq[String] = Seq(
        "POINT Z (0 2 1)",      "POINT Z (1 2 5)",      "POINT Z (2 2 37)",
        "POINT Z (0 1 21)",     "POINT Z (1 1 61)",     "POINT Z (2 1 129)",
        "POINT Z (0 0 97)",     "POINT Z (1 0 177)",    "POINT Z (2 0 221)",
    )
    val pointGeoms: Seq[MosaicGeometry] = points.map(p => GeometryAPI.apply("JTS").geometry(p, "WKT"))

    test("TestGDALRasterize: should rasterize point geometries using Z") {

        pointGeoms.foreach(_.setSpatialReference(4326))

        val origin = "POINT (0 2)" // top left-hand corner of north-up image
        val originGeom = GeometryAPI.apply("JTS").geometry(origin, "WKT").asInstanceOf[MosaicPoint]
        originGeom.setSpatialReference(4326)

        val raster = GDALRasterize.executeRasterize(pointGeoms, None, originGeom, 3, 3, 1.0, -1.0)

        val result = raster.getBand(1).values
        result shouldBe Seq(1, 5, 37, 21, 61, 129, 97, 177, 221)
    }

    test("TestGDALRasterize: should rasterize a point geometry using Z in a non-WGS84 projection") {
        pointGeoms.foreach(_.setSpatialReference(27700))
        val translatedPoints = pointGeoms.map(_.translate(348000.0, 462000.0))

        val origin = "POINT (0 2)" // top left-hand corner of north-up image
        val originGeom = GeometryAPI("JTS").geometry(origin, "WKT")
            .translate(348000.0, 462000.0)
            .asInstanceOf[MosaicPoint]
        originGeom.setSpatialReference(27700)

        val raster = GDALRasterize.executeRasterize(translatedPoints, None, originGeom, 3, 3, 1.0, -1.0)

        val result = raster.getBand(1).values
        result shouldBe Seq(1, 5, 37, 21, 61, 129, 97, 177, 221)
    }
}
