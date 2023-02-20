package com.databricks.labs.mosaic.core.geometry.api

import com.databricks.labs.mosaic.codegen.format.{MosaicGeometryIOCodeGenESRI, MosaicGeometryIOCodeGenJTS}
import com.databricks.labs.mosaic.core.geometry.point.{MosaicPointESRI, MosaicPointJTS}
import com.databricks.labs.mosaic.core.types.model.Coordinates
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class GeometryAPITest extends AnyFunSuite with GeometryAPIBehaviors {

    test("Construct available Geometry APIs") {
        noException should be thrownBy GeometryAPI("JTS")
        noException should be thrownBy GeometryAPI("ESRI")
        an[Error] should be thrownBy GeometryAPI("anyother")
        GeometryAPI.apply("JTS") shouldEqual JTS
        GeometryAPI.apply("ESRI") shouldEqual ESRI
    }

    test("JTS Geometry API") {
        val jts = GeometryAPI.apply("JTS")
        jts.name shouldEqual "JTS"
        jts.fromCoords(Seq(0.1, 0.2)).equals(MosaicPointJTS(Seq(0.1, 0.2))) shouldEqual true
        jts.fromGeoCoord(Coordinates(0.2, 0.1)).equals(MosaicPointJTS(Seq(0.1, 0.2))) shouldEqual true
        jts.ioCodeGen shouldEqual MosaicGeometryIOCodeGenJTS
        jts.codeGenTryWrap("1==1;").contains("try") shouldEqual true
    }

    test("ESRI Geometry API") {
        val esri = GeometryAPI.apply("ESRI")
        esri.name shouldEqual "ESRI"
        esri.fromCoords(Seq(0.1, 0.2)).equals(MosaicPointESRI(Seq(0.1, 0.2))) shouldEqual true
        esri.fromGeoCoord(Coordinates(0.2, 0.1)).equals(MosaicPointESRI(Seq(0.1, 0.2))) shouldEqual true
        esri.ioCodeGen shouldEqual MosaicGeometryIOCodeGenESRI
        esri.codeGenTryWrap("1==1;").contains("try") shouldEqual false
    }

    test("IllegalAPI Geometry API tests") {
        an[Error] should be thrownBy GeometryAPI.apply("anyother")
    }

    test("Geometry API serialize and deserialize") {
        serializeDeserializeBehavior("ESRI", MosaicPointESRI(Seq(0.1, 0.2)))
        serializeDeserializeBehavior("JTS", MosaicPointJTS(Seq(0.1, 0.2)))
    }

    test("Geometry API throw an exception when serializing non existing format.") {
        val point = MosaicPointESRI.fromWKT("POINT(1 1)")
        val geometryAPI = ESRI

        assertThrows[Error] {
            geometryAPI.serialize(point, "non-existent-format")
        }
    }

    test("Base signatures") {
        val jts: GeometryAPI = GeometryAPI.apply("JTS")
        val esri: GeometryAPI = GeometryAPI.apply("ESRI")
        noException should be thrownBy jts.fromCoords(Seq(0.1, 0.2))
        noException should be thrownBy esri.fromCoords(Seq(0.1, 0.2))
        noException should be thrownBy jts.fromGeoCoord(Coordinates(0.2, 0.1))
        noException should be thrownBy esri.fromGeoCoord(Coordinates(0.2, 0.1))
        noException should be thrownBy jts.ioCodeGen
        noException should be thrownBy esri.ioCodeGen
        noException should be thrownBy jts.codeGenTryWrap("1==1;")
        noException should be thrownBy esri.codeGenTryWrap("1==1;")
        noException should be thrownBy jts.geometryClass
        noException should be thrownBy esri.geometryClass
        noException should be thrownBy jts.mosaicGeometryClass
        noException should be thrownBy esri.mosaicGeometryClass
    }

}
