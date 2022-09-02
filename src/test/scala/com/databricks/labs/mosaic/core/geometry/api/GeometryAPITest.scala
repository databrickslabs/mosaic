package com.databricks.labs.mosaic.core.geometry.api

import com.databricks.labs.mosaic.codegen.format.{MosaicGeometryIOCodeGenESRI, MosaicGeometryIOCodeGenJTS}
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI._
import com.databricks.labs.mosaic.core.geometry.point.{MosaicPointESRI, MosaicPointJTS}
import com.databricks.labs.mosaic.core.types.model.GeoCoord
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.util.Locale

class GeometryAPITest extends AnyFunSuite with GeometryAPIBehaviors {

    test("Construct available Geometry APIs") {
        noException should be thrownBy GeometryAPI("JTS")
        noException should be thrownBy GeometryAPI("ESRI")
        noException should be thrownBy GeometryAPI("anyother")
        GeometryAPI.apply("JTS") shouldEqual JTS
        GeometryAPI.apply("ESRI") shouldEqual ESRI
        GeometryAPI.apply("anyother") shouldEqual IllegalAPI
    }

    test("JTS Geometry API") {
        val jts = GeometryAPI.apply("JTS")
        jts.name shouldEqual "JTS"
        jts.fromCoords(Seq(0.1, 0.2)).equals(MosaicPointJTS(Seq(0.1, 0.2))) shouldEqual true
        jts.fromGeoCoord(GeoCoord(0.2, 0.1)).equals(MosaicPointJTS(Seq(0.1, 0.2))) shouldEqual true
        jts.ioCodeGen shouldEqual MosaicGeometryIOCodeGenJTS
        jts.codeGenTryWrap("1==1;").contains("try") shouldEqual true
    }

    test("ESRI Geometry API") {
        val esri = GeometryAPI.apply("ESRI")
        esri.name shouldEqual "ESRI"
        esri.fromCoords(Seq(0.1, 0.2)).equals(MosaicPointESRI(Seq(0.1, 0.2))) shouldEqual true
        esri.fromGeoCoord(GeoCoord(0.2, 0.1)).equals(MosaicPointESRI(Seq(0.1, 0.2))) shouldEqual true
        esri.ioCodeGen shouldEqual MosaicGeometryIOCodeGenESRI
        esri.codeGenTryWrap("1==1;").contains("try") shouldEqual false
    }

    test("IllegalAPI Geometry API tests") {
        val illegalAPI = GeometryAPI.apply("anyother")
        illegalAPI.name.toUpperCase(Locale.ROOT) shouldEqual "ILLEGAL"
        an[Error] should be thrownBy illegalAPI.fromCoords(Seq(0.1, 0.2))
        an[Error] should be thrownBy illegalAPI.fromGeoCoord(GeoCoord(0.2, 0.1))
        an[Error] should be thrownBy illegalAPI.ioCodeGen
        an[Error] should be thrownBy illegalAPI.codeGenTryWrap("1==1;")
        GeometryAPI
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

}
