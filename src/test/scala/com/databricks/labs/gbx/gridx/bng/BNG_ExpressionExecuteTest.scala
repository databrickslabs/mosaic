package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.{be, not}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class BNG_ExpressionExecuteTest extends AnyFunSuite {

    test("BNG_AsWKB should return the WKB representation of the cell") {
        val cellId = "TQ388791"
        val cellIdLong = BNG.parse(cellId)
        val wkb = BNG_AsWKB.execute(cellId)
        val wkbLong = BNG_AsWKB.execute(cellIdLong)
        wkb shouldBe wkbLong
        wkb should not be null
        wkb.length should be > 0
        val geom = JTS.fromWKB(wkb)
        geom should not be null
        geom.getGeometryType should be("Polygon")
        geom.getArea should be > 0.0001
    }

    test("BNG_AsWKT should return the WKT representation of the cell") {
        val cellId = "TQ388791"
        val cellIdLong = BNG.parse(cellId)
        val wkt = BNG_AsWKT.execute(cellId)
        val wktLong = BNG_AsWKT.execute(cellIdLong)
        wkt shouldBe wktLong
        wkt should not be null
        wkt.length should be > 0
        val geom = JTS.fromWKT(wkt)
        geom should not be null
        geom.getGeometryType should be("Polygon")
        geom.getArea should be > 0.0001
    }

    test("BNG_Centroid should return the centroid of the cell") {
        val cellId = "TQ388791"
        val cellIdLong = BNG.parse(cellId)
        val centroid = BNG_Centroid.execute(cellId)
        val centroidLong = BNG_Centroid.execute(cellIdLong)
        centroid shouldBe centroidLong
        centroid should not be null
        val geom = JTS.fromWKB(centroid)
        geom should not be null
        geom.getGeometryType should be("Point")
    }

    test("BNG_CellArea should return the area of the cell") {
        val cellId = "TQ388791"
        val cellIdLong = BNG.parse(cellId)
        val area = BNG_CellArea.execute(cellId)
        val areaLong = BNG_CellArea.execute(cellIdLong)
        area shouldBe areaLong
        area should be > 0.0001
    }

    test("BNG_CellIntersection should return the intersection of two cells") {
        val cellId = "TQ388791"
        val cellId2 = "TQ388792"
        val cellIdLng = BNG.parse(cellId)
        val cellIdLng2 = BNG.parse(cellId2)
        val cellGeom = BNG.cellIdToGeometry(cellIdLng)
        val cellGeom2 = BNG.cellIdToGeometry(cellIdLng2)
        val cell1 = (cellIdLng, true, cellGeom)
        val cell2 = (cellIdLng2, true, cellGeom2)

        // Different cells should return empty intersection
        BNG_CellIntersection.executeLong(cell1, cell2)._3.isEmpty should be (true)
        // Same cell with one flagged as full should return the full cell
        val cell3 = (cellIdLng, false, cellGeom.buffer(-0.0001))
        val res3 = BNG_CellIntersection.executeLong(cell1, cell3)
        res3._2 should be (true)
        res3._1 should be (cellIdLng)
        res3._3.equalsTopo(cellGeom) should be (true)
        val res4 = BNG_CellIntersection.executeLong(cell3, cell1)
        res4._2 should be (true)
        res4._1 should be (cellIdLng)
        res4._3.equalsTopo(cellGeom) should be (true)
        // Same cell with both non-full should return the intersection
        val cell4 = (cellIdLng, false, cellGeom.buffer(-0.0001))
        val res5 = BNG_CellIntersection.executeLong(cell3, cell4)
        res5._2 should be (false)
        res5._1 should be (cellIdLng)
        res5._3.equalsTopo(cellGeom.buffer(-0.0001)) should be (true)

        // String method should work too
        val cell1s = (cellId, true, cellGeom)
        val cell2s = (cellId2, true, cellGeom2)
        BNG_CellUnion.executeString(cell1s, cell2s)._3.isEmpty should be (true)
    }

    test("BNG_CellUnion should return the union of two cells") {
        val cellId = "TQ388791"
        val cellId2 = "TQ388792"
        val cellIdLng = BNG.parse(cellId)
        val cellIdLng2 = BNG.parse(cellId2)
        val cellGeom = BNG.cellIdToGeometry(cellIdLng)
        val cellGeom2 = BNG.cellIdToGeometry(cellIdLng2)
        val cell1 = (cellIdLng, true, cellGeom)
        val cell2 = (cellIdLng2, true, cellGeom2)

        // Different cells should return empty intersection
        BNG_CellUnion.executeLong(cell1, cell2)._3.isEmpty should be (true)
        // Same cell with one flagged as full should return the full cell
        val cell3 = (cellIdLng, false, cellGeom.buffer(-0.0001))
        val res3 = BNG_CellUnion.executeLong(cell1, cell3)
        res3._2 should be (true)
        res3._1 should be (cellIdLng)
        res3._3.equalsTopo(cellGeom) should be (true)
        val res4 = BNG_CellUnion.executeLong(cell3, cell1)
        res4._2 should be (true)
        res4._1 should be (cellIdLng)
        res4._3.equalsTopo(cellGeom) should be (true)
        // Same cell with both non-full should return the union
        val cell4 = (cellIdLng, false, cellGeom.buffer(-0.0001))
        val res5 = BNG_CellUnion.executeLong(cell3, cell4)
        res5._2 should be (false)
        res5._1 should be (cellIdLng)
        res5._3.equalsTopo(cellGeom.buffer(-0.0001)) should be (true)

        // String method should work too
        val cell1s = (cellId, true, cellGeom)
        val cell2s = (cellId2, true, cellGeom2)
        BNG_CellUnion.executeString(cell1s, cell2s)._3.isEmpty should be (true)
    }

    test("BNG_Distance should return the distance between two cells") {
        val cellId = "TQ388791"
        val cellId2 = "TQ388792"
        val cellIdLng = BNG.parse(cellId)
        val cellIdLng2 = BNG.parse(cellId2)
        // Different cells should return a positive distance
        val dist1 = BNG_Distance.execute(cellId, cellId2)
        dist1 shouldBe 1L
        // Same cells should return zero distance
        val dist2 = BNG_Distance.execute(cellIdLng, cellIdLng2)
        dist2 shouldBe 1L
    }

    test("BNG_EastNorthAsBNG should return cell ID for easting/northing pair") {
        val east1 = 19922
        val north1 = 22219
        val cellId1 = BNG_EastNorthAsBNG.executeInt(east1, north1, 3)
        val cellId2 = BNG_EastNorthAsBNG.executeString(east1, north1, "50m")
        cellId1 shouldBe "SV1922"
        cellId2 shouldBe "SV199222SW"
    }

    test("BNG_EuclideanDistance should return the distance between two cells") {
        val cellId = "TQ388791"
        val cellId2 = "TQ389792"
        val cellIdLng = BNG.parse(cellId)
        val cellIdLng2 = BNG.parse(cellId2)
        // Different cells should return a positive distance
        val dist1 = BNG_EuclideanDistance.execute(cellId, cellId2)
        dist1 shouldBe 1L
        // Same cells should return zero distance
        val dist2 = BNG_EuclideanDistance.execute(cellIdLng, cellIdLng2)
        dist2 shouldBe 1L
    }

    test("BNG_GeometryKLoop should return the geometry based K-Loop") {
        val triangle = JTS.fromWKT("POLYGON ((10000 10000, 20000 10000, 20000 20000, 10000 10000))")
        val geomKLoop = BNG_GeometryKLoop.execute(triangle, 3, 2).toSeq
        geomKLoop.length shouldBe 52
    }

    test("BNG_GeometryKRing should return the geometry based K-Ring") {
        val triangle = JTS.fromWKT("POLYGON ((10000 10000, 20000 10000, 20000 20000, 10000 10000))")
        val geomKLoop = BNG_GeometryKRing.execute(triangle, 3, 2).toSeq
        geomKLoop.length shouldBe 151
    }

    test("BNG_KLoop should return cell IDs of the KLoop disk") {
        val cellID = "TQ388792"
        val kloop2 = BNG_KLoop.execute(cellID, 2).toSeq
        val cellID2 = BNG.parse(cellID)
        val kloop2_2 = BNG_KLoop.execute(cellID2, 2).toSeq
        kloop2.isEmpty should not be true
        kloop2_2.isEmpty should not be true
        kloop2.foreach(cell => BNG.euclideanDistance(BNG.parse(cell), cellID2) shouldBe 2)
    }

    test("BNG_KRing should return cell IDs of the KLoop disk") {
        val cellID = "TQ388792"
        val kloop2 = BNG_KRing.execute(cellID, 2).toSeq
        val cellID2 = BNG.parse(cellID)
        val kloop2_2 = BNG_KRing.execute(cellID2, 2).toSeq
        kloop2.isEmpty should not be true
        kloop2_2.isEmpty should not be true
        kloop2.foreach(cell => (BNG.euclideanDistance(BNG.parse(cell), cellID2) <= 2) shouldBe true)
    }

    test("BNG_PointAsBNG should return cell IDs of the point") {
        val cellId = BNG_PointAsBNG.executeWKT("POINT (199222 230330)", 3)
        cellId shouldBe "SM9930"
        val cellId2 = BNG_PointAsBNG.executeWKB(JTS.toWKB(JTS.fromWKT("POINT (199222 230330)")), 3)
        cellId2 shouldBe "SM9930"
    }

    test("BNG_Polyfill should return cell IDs of the poly fill") {
        val geom = JTS.fromWKT("POLYGON ((10000 10000, 20000 10000, 20000 20000, 10000 10000))")
        val polyfill1 = BNG_Polyfill.execute(geom, 3).toSeq
        val polyfill2 = BNG_Polyfill.execute(geom, "100m").toSeq
        polyfill1.isEmpty should not be true
        polyfill2.isEmpty should not be true
        polyfill1.length shouldBe 45
        polyfill2.length shouldBe 4950
    }

    test("BNG_Tessellate should return chips of the geometry") {
        val wkt = "POLYGON ((10000 10000, 20000 10000, 20000 20000, 10000 10000))"
        val wkb = JTS.toWKB(JTS.fromWKT(wkt))
        val tess1 = BNG_Tessellate.executeWKT(wkt, 3, keepCoreGeom = true).toSeq
        val tess2 = BNG_Tessellate.executeWKB(wkb, 3, bool = true).toSeq
        tess1.isEmpty should not be true
        tess2.isEmpty should not be true
        tess1.length shouldBe 55
        tess2.length shouldBe 55
        tess1.map(_._3).reduce(_.union(_)).equalsTopo(JTS.fromWKT(wkt)) shouldBe true
    }


}
