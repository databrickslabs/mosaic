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

        intersection.getNumGeometries should be(3)

    }

}
