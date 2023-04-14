package com.databricks.labs.mosaic.core.geometry

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class Test_MosaicGeometryJTS extends AnyFunSuite {

    test("Test_MosaicGeometryJTS Line Intersection - Issue 299") {
        val verticalLine = """{"type":"LineString","coordinates":[[1,0], [1,2]]}"""
        val horizontalLine = """{"type": "LineString", "coordinates": [[0,1], [2,1]]}"""

        val verticalLineGeom = MosaicGeometryJTS.fromJSON(verticalLine)
        val horizontalLineGeom = MosaicGeometryJTS.fromJSON(horizontalLine)

        val intersection = verticalLineGeom.intersection(horizontalLineGeom)
        intersection.isEmpty shouldBe false

    }

}
