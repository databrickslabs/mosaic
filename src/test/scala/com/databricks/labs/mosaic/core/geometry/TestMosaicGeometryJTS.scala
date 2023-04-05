package com.databricks.labs.mosaic.core.geometry

import com.databricks.labs.mosaic.JTS
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class TestMosaicGeometryJTS extends AnyFunSuite {

    test("MosaicGeometryJTS should return correct intersection - issue 309.") {
        // Fix for issue #309 https://github.com/databrickslabs/mosaic/issues/309
        val wkt = "POLYGON ((335500 177000, 336000 176700, 336000 176500, 335500 175800, 334800 176500, 334800 175500, 336200 175500," +
            " 336200 177000, 335500 177000))"

        val cellWkt = "POLYGON ((335000 176000, 336000 176000, 336000 177000, 335000 177000, 335000 176000, 335000 176000))"

        val geom = JTS.geometry(wkt, "WKT")
        val cellGeom = JTS.geometry(cellWkt, "WKT")

        val intersection = geom.intersection(cellGeom)

        // Returns 2 geometries, a line and a multipolygon, total pieces = 4
        intersection.flatten.map(_.getNumGeometries).sum should be(4)

    }

    test("MosaicGeometryJTS should return correct union for geometry collections.") {
        val geomCollection = JTS.geometry(
          "GEOMETRYCOLLECTION (" +
              " POLYGON ((1 1, 1 3, 2 3, 2 1, 1 1))," +
              " POLYGON ((4 3, 5 3, 5 4, 4 4, 4 3)))," +
              " LINESTRING ((0 5, 3 5, 4 6))," +
              " LINESTRING ((1 4, 2 4))" +
              ")",
          "WKT"
        )
        val otherGeom = JTS.geometry("POLYGON ((2 1.5, 4.5 1.5, 4.5 6, 2 6, 2 1.5))", "WKT")

        geomCollection.union(otherGeom).equals(otherGeom.union(geomCollection)) shouldBe true
    }

}
