package com.databricks.labs.mosaic.core.geometry.api

import com.databricks.labs.mosaic.codegen.format.MosaicGeometryIOCodeGenJTS
import com.databricks.labs.mosaic.core.geometry.point.{MosaicPointESRI, MosaicPointJTS}
import com.databricks.labs.mosaic.core.types.model.Coordinates
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class GeometryAPITest extends AnyFunSuite with GeometryAPIBehaviors {

    test("Construct available Geometry APIs") {
        noException should be thrownBy GeometryAPI("JTS")
        an[Error] should be thrownBy GeometryAPI("anyother")
        GeometryAPI.apply("JTS") shouldEqual JTS
    }

    test("JTS Geometry API") {
        val jts = GeometryAPI.apply("JTS")
        jts.name shouldEqual "JTS"
        jts.fromCoords(Seq(0.1, 0.2)).equals(MosaicPointJTS(Seq(0.1, 0.2))) shouldEqual true
        jts.fromGeoCoord(Coordinates(0.2, 0.1)).equals(MosaicPointJTS(Seq(0.1, 0.2))) shouldEqual true
        jts.ioCodeGen shouldEqual MosaicGeometryIOCodeGenJTS
        jts.codeGenTryWrap("1==1;").contains("try") shouldEqual true
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
        val geometryAPI = JTS

        assertThrows[Error] {
            geometryAPI.serialize(point, "non-existent-format")
        }
    }

    test("Base signatures") {
        val jts: GeometryAPI = GeometryAPI.apply("JTS")
        noException should be thrownBy jts.fromCoords(Seq(0.1, 0.2))
        noException should be thrownBy jts.fromGeoCoord(Coordinates(0.2, 0.1))
        noException should be thrownBy jts.ioCodeGen
        noException should be thrownBy jts.codeGenTryWrap("1==1;")
        noException should be thrownBy jts.geometryClass
        noException should be thrownBy jts.mosaicGeometryClass
    }

}
