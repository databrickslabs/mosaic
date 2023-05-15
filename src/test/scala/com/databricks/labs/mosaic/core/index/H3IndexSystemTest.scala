package com.databricks.labs.mosaic.core.index

import com.databricks.labs.mosaic.core.geometry.api.{ESRI, GeometryAPI, JTS}
import com.databricks.labs.mosaic.core.geometry.{MosaicGeometryESRI, MosaicGeometryJTS}
import com.databricks.labs.mosaic.core.index.H3IndexSystem.indexToGeometry
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.{LINESTRING, MULTILINESTRING, MULTIPOINT, MULTIPOLYGON, POINT, POLYGON}
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum
import com.uber.h3core.H3Core
import org.apache.spark.sql.types.{BooleanType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.scalactic.Tolerance
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter

class H3IndexSystemTest extends AnyFunSuite with Tolerance {

    test("H3IndexSystem auxiliary methods") {
        val indexRes = H3IndexSystem.pointToIndex(10, 10, 10)
        noException shouldBe thrownBy { H3IndexSystem.format(indexRes) }
        noException shouldBe thrownBy { H3IndexSystem.getResolutionStr(10) }
        noException shouldBe thrownBy { H3IndexSystem.indexToGeometry(H3IndexSystem.format(indexRes), JTS) }
        noException shouldBe thrownBy { H3IndexSystem.indexToGeometry(H3IndexSystem.format(indexRes), ESRI) }
        an[IllegalArgumentException] shouldBe thrownBy { H3IndexSystem.getResolution(true) }
        an[IllegalStateException] shouldBe thrownBy { H3IndexSystem.getResolution("-1") }
    }

    test("H3IndexSystem polyfill signatures") {
        val geomJTS = MosaicGeometryJTS.fromWKT("POLYGON((1 2, 2 2, 2 1, 1 1, 1 2))")
        val geomESRI = MosaicGeometryESRI.fromWKT("POLYGON((1 2, 2 2, 2 1, 1 1, 1 2))")
        noException shouldBe thrownBy { H3IndexSystem.polyfill(geomJTS, 10) }
        noException shouldBe thrownBy { H3IndexSystem.polyfill(geomESRI, 10) }
        noException shouldBe thrownBy { H3IndexSystem.polyfill(geomJTS, 10, Some(JTS)) }
        noException shouldBe thrownBy { H3IndexSystem.polyfill(geomESRI, 10, Some(ESRI)) }
    }

    test("H3IndexSystem inherited methods") {
        val cellId = H3IndexSystem.pointToIndex(10, 10, 10)
        val hexCellId = H3IndexSystem.format(cellId)
        val utfCellId = UTF8String.fromString(hexCellId)

        H3IndexSystem.formatCellId(cellId, LongType) shouldEqual cellId
        H3IndexSystem.formatCellId(hexCellId, LongType) shouldEqual cellId
        H3IndexSystem.formatCellId(utfCellId, LongType) shouldEqual cellId
        H3IndexSystem.formatCellId(cellId, StringType) shouldEqual hexCellId
        H3IndexSystem.formatCellId(hexCellId, StringType) shouldEqual hexCellId
        H3IndexSystem.formatCellId(utfCellId, StringType) shouldEqual hexCellId
        an[Error] should be thrownBy H3IndexSystem.formatCellId(true, StringType)

        H3IndexSystem.setCellIdDataType(BooleanType)
        an[Error] should be thrownBy H3IndexSystem.serializeCellId(cellId)

        H3IndexSystem.setCellIdDataType(StringType)
        H3IndexSystem.serializeCellId(cellId) shouldEqual utfCellId
        H3IndexSystem.serializeCellId(hexCellId) shouldEqual utfCellId
        H3IndexSystem.serializeCellId(utfCellId) shouldEqual utfCellId

        H3IndexSystem.setCellIdDataType(LongType)
        H3IndexSystem.serializeCellId(cellId) shouldEqual cellId
        H3IndexSystem.serializeCellId(hexCellId) shouldEqual cellId
        H3IndexSystem.serializeCellId(utfCellId) shouldEqual cellId
    }

    test("KRing should generate index IDs for string cell ID") {
        val cellId = H3IndexSystem.pointToIndex(10, 10, 10)
        val index = H3IndexSystem.format(cellId)
        val kRing1 = H3IndexSystem.asInstanceOf[IndexSystem].kRing(index, 1)
        val kRing2 = H3IndexSystem.asInstanceOf[IndexSystem].kRing(index, 2)
        val kRing3 = H3IndexSystem.asInstanceOf[IndexSystem].kRing(index, 3)

        kRing1 should contain theSameElementsAs H3IndexSystem.kRing(H3IndexSystem.parse(index), 1).map(H3IndexSystem.format)
        kRing2 should contain theSameElementsAs H3IndexSystem.kRing(H3IndexSystem.parse(index), 2).map(H3IndexSystem.format)
        kRing3 should contain theSameElementsAs H3IndexSystem.kRing(H3IndexSystem.parse(index), 3).map(H3IndexSystem.format)
    }

    test("KLoop should generate index IDs for string cell ID") {
        val cellId = H3IndexSystem.pointToIndex(10, 10, 10)
        val index = H3IndexSystem.format(cellId)
        val kLoop1 = H3IndexSystem.asInstanceOf[IndexSystem].kLoop(index, 1)
        val kLoop2 = H3IndexSystem.asInstanceOf[IndexSystem].kLoop(index, 2)
        val kLoop3 = H3IndexSystem.asInstanceOf[IndexSystem].kLoop(index, 3)

        kLoop1 should contain theSameElementsAs H3IndexSystem.kLoop(H3IndexSystem.parse(index), 1).map(H3IndexSystem.format)
        kLoop2 should contain theSameElementsAs H3IndexSystem.kLoop(H3IndexSystem.parse(index), 2).map(H3IndexSystem.format)
        kLoop3 should contain theSameElementsAs H3IndexSystem.kLoop(H3IndexSystem.parse(index), 3).map(H3IndexSystem.format)
    }

    test("Test coerce geometries") {
        val geomsWKTs1 = Seq(
            "POLYGON ((-0.1 51.5, -0.1 51.6, 0.1 51.6, 0.1 51.5, -0.1 51.5))",
            "MULTIPOLYGON (((-0.1 51.5, -0.1 51.6, 0.1 51.6, 0.1 51.5, -0.1 51.5)),((-0.1 51.5, -0.1 51.6, 0.1 51.6, 0.1 51.5, -0.1 51.5)))",
            "LINESTRING (-0.1 51.5, -0.1 51.6, 0.1 51.6, 0.1 51.5, -0.1 51.5)",
            "POINT (-0.1 51.5)"
        )
        val geomsWKTs2 = Seq(
            "LINESTRING (-0.1 51.5, -0.1 51.6, 0.1 51.6, 0.1 51.5, -0.1 51.5)",
            "MULTIPOINT (-0.1 51.5, -0.1 51.6, 0.1 51.6, 0.1 51.5, -0.1 51.5)",
            "LINESTRING (-0.1 51.5, -0.1 51.6, 0.1 51.6, 0.1 51.5, -0.1 51.5)"
        )
        val geomsWKTs3 = Seq(
            "POINT (-0.1 51.5)",
            "MULTIPOINT (-0.1 51.5, -0.1 51.6, 0.1 51.6, 0.1 51.5, -0.1 51.5)"
        )
        val geomsWKTs4 = Seq(
            "GEOMETRYCOLLECTION (POINT (-0.1 51.5), MULTIPOINT (-0.1 51.5, -0.1 51.6, 0.1 51.6, 0.1 51.5, -0.1 51.5))"
        )

        H3IndexSystem
            .coerceChipGeometry(geomsWKTs1.map(MosaicGeometryJTS.fromWKT))
            .map(g => GeometryTypeEnum.fromString(g.getGeometryType))
            .forall(Seq(POLYGON, MULTIPOLYGON).contains(_)) shouldBe true
        H3IndexSystem
            .coerceChipGeometry(geomsWKTs1.map(MosaicGeometryESRI.fromWKT))
            .map(g => GeometryTypeEnum.fromString(g.getGeometryType))
            .forall(Seq(POLYGON, MULTIPOLYGON).contains(_)) shouldBe true
        H3IndexSystem
            .coerceChipGeometry(geomsWKTs2.map(MosaicGeometryJTS.fromWKT))
            .map(g => GeometryTypeEnum.fromString(g.getGeometryType))
            .forall(Seq(LINESTRING, MULTILINESTRING).contains(_)) shouldBe true
        H3IndexSystem
            .coerceChipGeometry(geomsWKTs2.map(MosaicGeometryESRI.fromWKT))
            .map(g => GeometryTypeEnum.fromString(g.getGeometryType))
            .forall(Seq(LINESTRING, MULTILINESTRING).contains(_)) shouldBe true
        H3IndexSystem
            .coerceChipGeometry(geomsWKTs3.map(MosaicGeometryJTS.fromWKT))
            .map(g => GeometryTypeEnum.fromString(g.getGeometryType))
            .forall(Seq(POINT, MULTIPOINT).contains(_)) shouldBe true
        H3IndexSystem
            .coerceChipGeometry(geomsWKTs3.map(MosaicGeometryESRI.fromWKT))
            .map(g => GeometryTypeEnum.fromString(g.getGeometryType))
            .forall(Seq(POINT, MULTIPOINT).contains(_)) shouldBe true
        H3IndexSystem.coerceChipGeometry(geomsWKTs4.map(MosaicGeometryJTS.fromWKT)).isEmpty shouldBe true
        H3IndexSystem.coerceChipGeometry(geomsWKTs4.map(MosaicGeometryESRI.fromWKT)).isEmpty shouldBe true
    }

    test("indexToGeometry should return valid and correct geometries") {
        val h3: H3Core = H3Core.newInstance()

        val esriGeomAPI: GeometryAPI = GeometryAPI("ESRI")
        val jtsGeomAPI: GeometryAPI = GeometryAPI("JTS")
        val apis = Seq(esriGeomAPI, jtsGeomAPI)

        val baseCells = h3.getRes0Indexes.asScala.toList
        val lvl1Cells = baseCells.flatMap(h3.h3ToChildren(_, 1).asScala)
        val testCells = Seq(baseCells, lvl1Cells)

        val baseCellsStr = baseCells.map(h3.h3ToString(_))
        val lvl1CellsStr = lvl1Cells.map(h3.h3ToString(_))
        val testCellsStr = Seq(baseCellsStr, lvl1CellsStr)

        apis.foreach(api => {
            testCells.foreach(cells => {
                val geoms = cells.map(indexToGeometry(_, api))
                geoms.foreach(geom => geom.isValid shouldBe true)
                geoms.foldLeft(0.0)((acc, geom) => acc + geom.getArea) shouldBe ((180.0 * 360.0) +- 0.0001)
            })
            testCellsStr.foreach(cells => {
                val geoms = cells.map(indexToGeometry(_, api))
                geoms.foreach(geom => geom.isValid shouldBe true)
                geoms.foldLeft(0.0)((acc, geom) => acc + geom.getArea) shouldBe ((180.0 * 360.0) +- 0.0001)
            })
        })
    }
}
