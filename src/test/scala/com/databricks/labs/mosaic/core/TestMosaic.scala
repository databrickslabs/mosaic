package com.databricks.labs.mosaic.core

import com.databricks.labs.mosaic.{H3, JTS}
import com.databricks.labs.mosaic.core.index._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class TestMosaic extends AnyFunSuite {

    test("mosaicFill should not return duplicates with H3") {
        // This tests the fix for issue #243 https://github.com/databrickslabs/mosaic/issues/243
        val geom = JTS.geometry(
          "POLYGON ((4.42 51.78, 4.38 51.78, 4.39 51.83, 4.40 51.83, 4.41 51.8303, 4.417 51.8295, 4.42 51.83, 4.44 51.81, 4.42 51.78))",
          "WKT"
        )
        val result = Mosaic.mosaicFill(geom, 7, keepCoreGeom = true, H3IndexSystem, JTS)

        assert(result.length == 10)
        assert(result.map(x => x.index).distinct.length == 10)
    }

    test("Polygon should return a k-ring") {
        val polygon =
            "POLYGON ((-73.15203987512825 41.65493888808187, -73.15276005327304 41.654464534276144, -73.1534774913585 41.65398408823101," +
                " -73.15419392499267 41.65350553754797, -73.15490927428783 41.65302455163131, -73.15562571110856 41.65254793798908," +
                " -73.15634368724263 41.65206744602062, -73.15705807062369 41.65159004880297, -73.15731239180201 41.65141993549811," +
                " -73.15777654592605 41.65110813492959, -73.15849148815478 41.65062931467526, -73.15920857202283 41.650150840155845," +
                " -73.15992302044982 41.64967211027974, -73.16063898461574 41.64919378876085, -73.16135519740746 41.648713437678566," +
                " -73.16207164924467 41.64823497334122, -73.1627885741791 41.647755144686975, -73.16350625871115 41.647275319743144," +
                " -73.16422169735793 41.64679722055354, -73.16494134072073 41.64631906203407, -73.16565369192317 41.64583842224602," +
                " -73.16636983326426 41.645358003568795, -73.16708766647704 41.64487976843594, -73.16780256963874 41.644399682754724," +
                " -73.16854605445398 41.643902823959415, -73.17033751716234 41.644983274281614, -73.17253823425115 41.64631261117375," +
                " -73.17193389459365 41.646894728269224, -73.17135689834309 41.64745620467151, -73.17076909435258 41.648015175832654," +
                " -73.17018706340953 41.64857422903216, -73.1696037457729 41.649134448673664, -73.16902267733688 41.649696981284," +
                " -73.16843897396495 41.65025478289252, -73.16785747123949 41.65081511159219, -73.1672752495212 41.651375887324555," +
                " -73.16669360977347 41.65193549858073, -73.16610628498582 41.652494188624, -73.16552602032272 41.653055406663334," +
                " -73.16494093450561 41.653618385728294, -73.16436105223679 41.65417640070095, -73.16377902055123 41.65473719420326," +
                " -73.16319536474295 41.65529794703389, -73.1626200103856 41.65586153432125, -73.16202888149982 41.656416992938276," +
                " -73.1614469289715 41.656978874154134, -73.16086400864474 41.65753817612168, -73.16028157563906 41.658091666569824," +
                " -73.16000331309866 41.658357439331475, -73.15979488017096 41.65853007132134, -73.15923429499004 41.65899462327653," +
                " -73.15902680427884 41.65916030197158, -73.15691038908221 41.6578889582749, -73.15478938601524 41.6566036161547," +
                " -73.15203987512825 41.65493888808187))"
        val geom = JTS.geometry(polygon, "WKT")
        val conf = GridConf(-180, 180, -90, 90, 2, 360, 180)
        val grid = CustomIndexSystem(conf)
        val result = Mosaic.geometryKRing(geom, 7, 1, grid, JTS)

        assert(result.nonEmpty)
    }

    test("MosaicFill should return correct tiles for shapes aligned with the grid.") {
        // Fix for issue #309 https://github.com/databrickslabs/mosaic/issues/309
        val wkt = "POLYGON ((335500 177000, 336000 176700, 336000 176500, 335500 175800, 334800 176500, 334800 175500, 336200 175500," +
            " 336200 177000, 335500 177000))"

        val geom = JTS.geometry(wkt, "WKT")

        val result = Mosaic.mosaicFill(geom, BNGIndexSystem.resolutionMap("1km"), keepCoreGeom = true, BNGIndexSystem, JTS)

        val chipArea = result.map(_.geom.getArea).sum
        val expectedArea = geom.getArea

        math.abs(chipArea - expectedArea) should be < 1e-8

    }

    test("MosaicFill should not return empty set for bounding box.") {
        val wkt = "POLYGON (( -127.48832860406948 -0.0011265364650581968, " +
            "-127.48832851537821 -0.0010988881177292488, -127.4883092423591 -0.0010984513060565016," +
            " -127.48830933794375 -0.0011272500508798704, -127.48832860406948 -0.0011265364650581968))"

        val bbox = JTS.geometry(wkt, "WKT")

        val cells = Mosaic
            .mosaicFill(bbox, 6, keepCoreGeom = false, H3, JTS)
            .map(_.indexAsLong(H3))

        cells.length should be > 0
    }

}
