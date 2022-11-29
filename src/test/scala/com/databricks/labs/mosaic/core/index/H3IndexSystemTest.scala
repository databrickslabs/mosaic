package com.databricks.labs.mosaic.core.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.geometry.{MosaicGeometryESRI, MosaicGeometryJTS}
import org.apache.spark.sql.types.{BooleanType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class H3IndexSystemTest extends AnyFunSuite {

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

}
