package com.dblabs.spatial.gridx.bng

import com.dblabs.spatial.gridx.grid.BNG
import com.dblabs.spatial.vectorx.jts.JTS
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
        val cell1 = (true, cellIdLng, cellGeom)
        val cell2 = (true, cellIdLng2, cellGeom2)

        // Different cells should return empty intersection
        BNG_CellIntersection.executeLong(cell1, cell2)._3.isEmpty should be (true)
        // Same cell with one flagged as full should return the full cell
        val cell3 = (false, cellIdLng, cellGeom.buffer(-0.0001))
        val res3 = BNG_CellIntersection.executeLong(cell1, cell3)
        res3._1 should be (true)
        res3._2 should be (cellIdLng)
        res3._3.equalsTopo(cellGeom) should be (true)
        val res4 = BNG_CellIntersection.executeLong(cell3, cell1)
        res4._1 should be (true)
        res4._2 should be (cellIdLng)
        res4._3.equalsTopo(cellGeom) should be (true)
        // Same cell with both non-full should return the intersection
        val cell4 = (false, cellIdLng, cellGeom.buffer(-0.0001))
        val res5 = BNG_CellIntersection.executeLong(cell3, cell4)
        res5._1 should be (false)
        res5._2 should be (cellIdLng)
        res5._3.equalsTopo(cellGeom.buffer(-0.0001)) should be (true)

    }


}
