package com.databricks.labs.mosaic.core.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.JTS
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class TestCustomIndexSystem extends AnyFunSuite {

    test("Point to Index should generate index ID for resolution 0") {

        val conf = GridConf(0, 100, 0, 100, 2, 2)

        val grid = new CustomIndexSystem(conf)
        val resolutionMask = 0x00.toLong

        grid.pointToIndex(51, 51, 0) shouldBe 0 | resolutionMask
    }

    test("Point to Index should generate index ID for resolution 1") {

        val conf = GridConf(0, 100, 0, 100, 2, 2)

        val grid = new CustomIndexSystem(conf)
        val resolutionMask = 0x01.toLong << 56

        // First quadrant
        grid.pointToIndex(0, 0, 1) shouldBe 0 | resolutionMask
        grid.pointToIndex(0, 1, 1) shouldBe 0 | resolutionMask
        grid.pointToIndex(1, 0, 1) shouldBe 0 | resolutionMask

        // Second quadrant
        grid.pointToIndex(50, 0, 1) shouldBe 1 | resolutionMask
        grid.pointToIndex(51, 0, 1) shouldBe 1 | resolutionMask

        // Third quadrant
        grid.pointToIndex(0, 51, 1) shouldBe 2 | resolutionMask
        grid.pointToIndex(0, 50, 1) shouldBe 2 | resolutionMask

        // Second quadrant
        grid.pointToIndex(51, 51, 1) shouldBe 3 | resolutionMask

        // TODO: manage border case
//        grid.pointToIndex(100, 100, 1) shouldBe 3 | resolutionMask
    }


    test("Point to Index should generate index ID for resolution 2") {

        val conf = GridConf(0, 100, 0, 100, 2, 2)

        val grid = new CustomIndexSystem(conf)
        val resolutionMask = 0x02.toLong << 56

        // First quadrant
        grid.pointToIndex(0, 0, 2) shouldBe 0 | resolutionMask
        grid.pointToIndex(25, 0, 2) shouldBe 1 | resolutionMask
        grid.pointToIndex(0, 25, 2) shouldBe 4 | resolutionMask
    }

    test("Point to Index should generate index ID for resolution 1 on origin-offset grid") {

        val conf = GridConf(-100, 100, -10, 100, 2, 2)

        val grid = new CustomIndexSystem(conf)
        val resolutionMask = 0x01.toLong << 56

        // First quadrant
        grid.pointToIndex(-100, -10, 1) shouldBe 0 | resolutionMask
        grid.pointToIndex(-1, -1, 1) shouldBe 0 | resolutionMask

        // Second quadrant
        grid.pointToIndex(0, -10, 1) shouldBe 1 | resolutionMask
        grid.pointToIndex(0, 44, 1) shouldBe 1 | resolutionMask

        // Third quadrant
        grid.pointToIndex(-100, 45, 1) shouldBe 2 | resolutionMask
        grid.pointToIndex(-100, 99, 1) shouldBe 2 | resolutionMask
        grid.pointToIndex(-1, 45, 1) shouldBe 2 | resolutionMask
        grid.pointToIndex(-1, 99, 1) shouldBe 2 | resolutionMask

    }


    test("Index to geometry") {

        val conf = GridConf(0, 100, 0, 100, 2, 2)

        val grid = new CustomIndexSystem(conf)
        val resolutionMask = 0x01.toLong << 56

        // First quadrant
        val wkt0 = grid.indexToGeometry(0 | resolutionMask, JTS).toWKT
        wkt0 shouldBe "POLYGON ((0 0, 50 0, 50 50, 0 50, 0 0, 0 0))"

        val wkt1 = grid.indexToGeometry(1 | resolutionMask, JTS).toWKT
        wkt1 shouldBe "POLYGON ((50 0, 100 0, 100 50, 50 50, 50 0, 50 0))"

        val wkt2 = grid.indexToGeometry(2 | resolutionMask, JTS).toWKT
        wkt2 shouldBe "POLYGON ((0 50, 50 50, 50 100, 0 100, 0 50, 0 50))"

        val wkt3 = grid.indexToGeometry(3 | resolutionMask, JTS).toWKT
        wkt3 shouldBe "POLYGON ((50 50, 100 50, 100 100, 50 100, 50 50, 50 50))"
    }

    test("polyfill single cell") {
        val conf = GridConf(0, 100, 0, 100, 2, 2)

        val grid = new CustomIndexSystem(conf)
        val resolutionMask = 0x01.toLong << 56

        val geom = JTS.geometry("POLYGON ((0 0, 50 0, 50 50, 0 50, 0 0))", "WKT")
        grid.polyfill(geom, 1, Some(JTS)).toSet shouldBe Set(0 | resolutionMask)

        // Geometry which cell center does not fall into does not get selected
        val geomSmall = JTS.geometry("POLYGON ((30 30, 40 30, 40 40, 30 40, 30 30))", "WKT")
        grid.polyfill(geomSmall, 1, Some(JTS)).toSet shouldBe Set()

        // Small geometry for which the cell center falls within should be detected
        val geomCentered = JTS.geometry("POLYGON ((24 24, 26 24, 26 26, 24 26, 24 24))", "WKT")
        grid.polyfill(geomCentered, 1, Some(JTS)).toSet shouldBe Set(0 | resolutionMask)

    }

    test("polyfill multi cell") {
        val conf = GridConf(0, 100, 0, 100, 2, 2)

        val grid = new CustomIndexSystem(conf)
        val resolutionMask = 0x01.toLong << 56

        // Small geometry that spans multiple cels should be detected
        val geomMultiCell = JTS.geometry("POLYGON ((24 24, 76 24, 76 76, 24 76, 24 24))", "WKT")
        grid.polyfill(geomMultiCell, 1, Some(JTS)).toSet shouldBe Set(
            0 | resolutionMask,
            1 | resolutionMask,
            2 | resolutionMask,
            3 | resolutionMask,
        )

        // Small geometry that spans multiple cels should be detected
        val geomAlmostMultiCell = JTS.geometry("POLYGON ((25 25, 75 25, 75 75, 25 75, 25 25))", "WKT")
        grid.polyfill(geomAlmostMultiCell, 1, Some(JTS)).toSet shouldBe Set()
    }
}
