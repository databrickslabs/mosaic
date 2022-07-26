package com.databricks.labs.mosaic.core.geometry.api

import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint
import com.databricks.labs.mosaic.core.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.{an, convertToAnyShouldWrapper}

trait GeometryAPIBehaviors { this: AnyFunSuite =>

    def serializeDeserializeBehavior(geometryAPIName: String, point: MosaicPoint): Unit = {
        val geometryAPI = GeometryAPI.apply(geometryAPIName)

        val wktRow = InternalRow.fromSeq(Seq(UTF8String.fromString(point.toWKT)))
        val wkbRow = InternalRow.fromSeq(Seq(point.toWKB))
        val hexRow = InternalRow.fromSeq(Seq(geometryAPI.serialize(point, HexType)))
        val geojsonRow = InternalRow.fromSeq(Seq(geometryAPI.serialize(point, JSONType)))
        val coordsRow = InternalRow.fromSeq(Seq(geometryAPI.serialize(point, InternalGeometryType)))

        val wktSer = geometryAPI.serialize(point, "WKT")
        val wkbSer = geometryAPI.serialize(point, "WKB")
        val hexSer = geometryAPI.serialize(point, "HEX")
        val jsonSer = geometryAPI.serialize(point, "JSONOBJECT")
        val coordsSer = geometryAPI.serialize(point, "COORDS")

        val wktSerDT = geometryAPI.serialize(point, StringType)
        val wkbSerDT = geometryAPI.serialize(point, BinaryType)
        val hexSerDT = geometryAPI.serialize(point, HexType)
        val jsonSerDT = geometryAPI.serialize(point, JSONType)
        val coordsSerDT = geometryAPI.serialize(point, InternalGeometryType)

        an[Error] should be thrownBy geometryAPI.serialize(point, "illegal")
        an[Error] should be thrownBy geometryAPI.serialize(point, IntegerType)
        an[Error] should be thrownBy geometryAPI.geometry("POINT (1 1)", IntegerType)
        an[Error] should be thrownBy geometryAPI.geometry(wktRow, IntegerType)
        an[Error] should be thrownBy geometryAPI.geometry(point.toInternal, "COORDS")
        an[Error] should be thrownBy geometryAPI.geometry(point.toInternal, "illegal")

        geometryAPI.geometry(wktSer, StringType).equals(point) shouldEqual true
        geometryAPI.geometry(wkbSer, BinaryType).equals(point) shouldEqual true
        geometryAPI.geometry(hexSer, HexType).equals(point) shouldEqual true
        geometryAPI.geometry(jsonSer, JSONType).equals(point) shouldEqual true
        geometryAPI.geometry(coordsSer, InternalGeometryType).equals(point) shouldEqual true

        geometryAPI.geometry(wktSerDT, StringType).equals(point) shouldEqual true
        geometryAPI.geometry(wkbSerDT, BinaryType).equals(point) shouldEqual true
        geometryAPI.geometry(hexSerDT, HexType).equals(point) shouldEqual true
        geometryAPI.geometry(jsonSerDT, JSONType).equals(point) shouldEqual true
        geometryAPI.geometry(coordsSerDT, InternalGeometryType).equals(point) shouldEqual true

        geometryAPI.geometry(point.toWKB, "WKB").equals(point) shouldEqual true
        geometryAPI.geometry(point.toWKT, "WKT").equals(point) shouldEqual true
        geometryAPI.geometry(point.toHEX, "HEX").equals(point) shouldEqual true
        geometryAPI.geometry(point.toJSON, "GEOJSON").equals(point) shouldEqual true
        an[Error] should be thrownBy geometryAPI.geometry(point.toInternal, "COORDS")

        geometryAPI.geometry(wkbRow, BinaryType).equals(point) shouldEqual true
        geometryAPI.geometry(wktRow, StringType).equals(point) shouldEqual true
        geometryAPI.geometry(hexRow, HexType).equals(point) shouldEqual true
        geometryAPI.geometry(geojsonRow, JSONType).equals(point) shouldEqual true
        geometryAPI.geometry(coordsRow, InternalGeometryType).equals(point) shouldEqual true
        an[Error] should be thrownBy geometryAPI.geometry(wkbRow, IntegerType)
    }

}
