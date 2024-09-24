package com.databricks.labs.mosaic.core.index

import com.databricks.labs.mosaic.JTS
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class TestCustomIndexSystem extends AnyFunSuite {

    test("Point to Index should generate index ID for resolution 0") {

        val conf = GridConf(0, 100, 0, 100, 2, 100, 100)

        val grid = CustomIndexSystem(conf)
        val resolutionMask = 0x00.toLong

        grid.pointToIndex(51, 51, 0) shouldBe 0 | resolutionMask
    }

    test("Point to Index should generate index ID for resolution 1") {

        val conf = GridConf(0, 100, 0, 100, 2, 100, 100)

        val grid = CustomIndexSystem(conf)
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

        val conf = GridConf(0, 100, 0, 100, 2, 100, 100)

        val grid = CustomIndexSystem(conf)
        val resolutionMask = 0x02.toLong << 56

        // First quadrant
        grid.pointToIndex(0, 0, 2) shouldBe 0 | resolutionMask
        grid.pointToIndex(25, 0, 2) shouldBe 1 | resolutionMask
        grid.pointToIndex(0, 25, 2) shouldBe 4 | resolutionMask
    }

    test("Point to Index should generate index ID for resolution 1 on origin-offset grid") {

        val conf = GridConf(-100, 100, -10, 100, 2, 200, 110)

        val grid = CustomIndexSystem(conf)
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


    test("Point to Index should generate index ID for resolution 1 on 10x10 root cell size") {

        val conf = GridConf(0, 100, 0, 100, 2, 10, 10)

        val grid = CustomIndexSystem(conf)
        val resolutionMask = 0x01.toLong << 56

        grid.pointToIndex(0, 2, 1) shouldBe 0 | resolutionMask
        grid.pointToIndex(6, 0, 1) shouldBe 1 | resolutionMask
        grid.pointToIndex(0, 6, 1) shouldBe 20 | resolutionMask

    }

    test("Point to Index should generate the correct index ID for grids not multiple of root cells") {

        val conf = GridConf(441000, 900000, 6040000, 6410000, 10, 100000, 100000)

        val grid = CustomIndexSystem(conf)

        val p1 = grid.pointToIndex(558115, 6338615, 4)
        val p2 = grid.pointToIndex(558115, 6338625, 4)
        val p3 = grid.pointToIndex(558125, 6338615, 4)

        p1 should not equal p2
        p1 should not equal p3
        p2 should not equal p3
    }

    test("Index to geometry") {

        val conf = GridConf(0, 100, 0, 100, 2, 100, 100)

        val grid = CustomIndexSystem(conf)
        val resolutionMask = 0x01.toLong << 56

        // First quadrant
        val wkt0 = grid.indexToGeometry(0 | resolutionMask, JTS).toWKT
        wkt0 shouldBe "POLYGON ((0 0, 50 0, 50 50, 0 50, 0 0))"

        val wkt1 = grid.indexToGeometry(1 | resolutionMask, JTS).toWKT
        wkt1 shouldBe "POLYGON ((50 0, 100 0, 100 50, 50 50, 50 0))"

        val wkt2 = grid.indexToGeometry(2 | resolutionMask, JTS).toWKT
        wkt2 shouldBe "POLYGON ((0 50, 50 50, 50 100, 0 100, 0 50))"

        val wkt3 = grid.indexToGeometry(3 | resolutionMask, JTS).toWKT
        wkt3 shouldBe "POLYGON ((50 50, 100 50, 100 100, 50 100, 50 50))"
    }

    test("polyfill single cell") {
        val conf = GridConf(0, 100, 0, 100, 2, 100, 100)

        val grid = CustomIndexSystem(conf)
        val resolutionMask = 0x01.toLong << 56

        val geom = JTS.geometry("POLYGON ((0 0, 50 0, 50 50, 0 50, 0 0))", "WKT")
        grid.polyfill(geom, 1, JTS).toSet shouldBe Set(0 | resolutionMask)

        // Geometry which cell center does not fall into does not get selected
        val geomSmall = JTS.geometry("POLYGON ((30 30, 40 30, 40 40, 30 40, 30 30))", "WKT")
        grid.polyfill(geomSmall, 1, JTS).toSet shouldBe Set()

        // Small geometry for which the cell center falls within should be detected
        val geomCentered = JTS.geometry("POLYGON ((24 24, 26 24, 26 26, 24 26, 24 24))", "WKT")
        grid.polyfill(geomCentered, 1, JTS).toSet shouldBe Set(0 | resolutionMask)

    }

    test("polyfill single cell with negative coordinates") {
        val conf = GridConf(-100, 100, -100, 100, 2, 200, 200)

        val grid = CustomIndexSystem(conf)
        val resolution = 3
        val resolutionMask = resolution.toLong << 56

        // At resolution = 3, the cell splits are at: -100, -75, -50, -25, 0, 25, 50, 75, 100

        grid.getCellWidth(resolution) shouldBe 25.0
        grid.getCellHeight(resolution) shouldBe 25.0

        grid.getCellPositionFromCoordinates(1.0, 1.0, resolution) shouldBe (4, 4, 36)

        val geom = JTS.geometry("POLYGON ((0 0, 25 0, 25 25, 0 25, 0 0))", "WKT")
        grid.polyfill(geom, resolution, JTS).toSet shouldBe Set(36 | resolutionMask)

        // Geometry which cell center does not fall into does not get selected
        val geomSmall = JTS.geometry("POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))", "WKT")
        grid.polyfill(geomSmall, resolution, JTS).toSet shouldBe Set()

        // Small geometry for which the cell center falls within should be detected
        val geomCentered = JTS.geometry("POLYGON ((12 12, 13 12, 13 13, 12 13, 12 12))", "WKT")
        grid.polyfill(geomCentered, resolution, JTS).toSet shouldBe Set(36 | resolutionMask)

    }

    test("polyfill single cell with world coordinates") {
        val conf = GridConf(-180, 180, -90, 90, 2, 360, 180)

        val grid = CustomIndexSystem(conf)
        val resolution = 3
        val resolutionMask = resolution.toLong << 56

        // At resolution = 3, the cell splits are at: -180, -135, -90, -45, 0, 45, 90, 135, 180
        //                                            -90, -67.5, -45, -22.5, 0, 22.5, 45, 67.5, 90

        grid.getCellWidth(resolution) shouldBe 45.0
        grid.getCellHeight(resolution) shouldBe 22.5

        grid.getCellPositionFromCoordinates(1.0, 1.0, resolution) shouldBe (4, 4, 36)

        val geom = JTS.geometry("POLYGON ((-95 9, -50 9, -50 32, -95 32, -95 9))", "WKT")
        grid.polyfill(geom, resolution, JTS).toSet shouldBe Set(34 | resolutionMask)

    }

    test("polyfill multi cell") {
        val conf = GridConf(0, 100, 0, 100, 2, 100, 100)

        val grid = CustomIndexSystem(conf)
        val resolutionMask = 0x01.toLong << 56

        // Small geometry that spans multiple cels should be detected
        val geomMultiCell = JTS.geometry("POLYGON ((24 24, 76 24, 76 76, 24 76, 24 24))", "WKT")
        grid.polyfill(geomMultiCell, 1, JTS).toSet shouldBe Set(
            0 | resolutionMask,
            1 | resolutionMask,
            2 | resolutionMask,
            3 | resolutionMask,
        )

        // Small geometry that spans multiple cels should be detected
        val geomAlmostMultiCell = JTS.geometry("POLYGON ((25 25, 75 25, 75 75, 25 75, 25 25))", "WKT")
        grid.polyfill(geomAlmostMultiCell, 1, JTS).toSet shouldBe Set()
    }


    test("polyfill single DK grid") {
        val conf = GridConf(441000, 900000, 6048000, 6410000, 10, 100000, 100000)

        val grid = CustomIndexSystem(conf)

        val geom = JTS.geometry("POLYGON ((528435.784 6142513.9146, 528428.2785999998 6142513.0317, 528419.4486999996 6142513.9146, 528408.8529000003 6142513.9146, 528401.5744000003 6142515.895300001, 528396.9325000001 6142520.537, 528394.2835999997 6142526.718, 528393.4006000003 6142532.4574, 528395.608 6142538.6383, 528397.8154999996 6142540.845799999, 528402.2304999996 6142541.728800001, 528405.3208999997 6142542.1702, 528409.7357999999 6142543.053200001, 528414.5922999997 6142543.4947, 528418.5657000002 6142545.702199999, 528422.9807000002 6142548.7927, 528424.3051000005 6142552.766100001, 528429.1616000002 6142554.9736, 528433.1349999998 6142556.739499999, 528436.2254999997 6142555.8566, 528441.5234000003 6142554.090600001, 528445.3328999998 6142546.090700001, 528449.0288000004 6142536.872300001, 528448.6368000004 6142527.072899999, 528442.8479000004 6142517.8881, 528435.784 6142513.9146))", "WKT")
        assert(grid.polyfill(geom, 4, JTS).nonEmpty)

    }
}
