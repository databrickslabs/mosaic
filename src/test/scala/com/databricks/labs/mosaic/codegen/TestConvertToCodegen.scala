package com.databricks.labs.mosaic.codegen

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkCodeGenSuite
import org.scalatest.flatspec.AnyFlatSpec

class TestConvertToCodegen extends AnyFlatSpec with ConvertToCodegenBehaviors with SparkCodeGenSuite {

    "ConvertTo Expression from WKB to WKT" should "do codegen for any index system and any geometry API" in {
        it should behave like codegenWKBtoWKT(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like codegenWKBtoWKT(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ConvertTo Expression from WKB to HEX" should "do codegen for any index system and any geometry API" in {
        it should behave like codegenWKBtoHEX(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like codegenWKBtoHEX(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ConvertTo Expression from WKB to COORDS" should "do codegen for any index system and any geometry API" in {
        it should behave like codegenWKBtoCOORDS(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like codegenWKBtoCOORDS(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ConvertTo Expression from WKB to GEOJSON" should "do codegen for any index system and any geometry API" in {
        it should behave like codegenWKBtoGEOJSON(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like codegenWKBtoGEOJSON(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ConvertTo Expression from WKT to WKB" should "do codegen for any index system and any geometry API" in {
        it should behave like codegenWKTtoWKB(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like codegenWKTtoWKB(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ConvertTo Expression from WKT to HEX" should "do codegen for any index system and any geometry API" in {
        it should behave like codegenWKTtoHEX(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like codegenWKTtoHEX(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ConvertTo Expression from WKT to COORDS" should "do codegen for any index system and any geometry API" in {
        it should behave like codegenWKTtoCOORDS(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like codegenWKTtoCOORDS(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ConvertTo Expression from WKT to GeoJSON" should "do codegen for any index system and any geometry API" in {
        it should behave like codegenWKTtoGEOJSON(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like codegenWKTtoGEOJSON(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ConvertTo Expression from HEX to WKB" should "do codegen for any index system and any geometry API" in {
        it should behave like codegenHEXtoWKB(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like codegenHEXtoWKB(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ConvertTo Expression from HEX to WKT" should "do codegen for any index system and any geometry API" in {
        it should behave like codegenHEXtoWKT(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like codegenHEXtoWKT(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ConvertTo Expression from HEX to COORDS" should "do codegen for any index system and any geometry API" in {
        it should behave like codegenWKTtoCOORDS(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like codegenWKTtoCOORDS(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ConvertTo Expression from HEX to GEOJSON" should "do codegen for any index system and any geometry API" in {
        it should behave like codegenHEXtoGEOJSON(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like codegenHEXtoGEOJSON(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ConvertTo Expression from COORDS to WKB" should "do codegen for any index system and any geometry API" in {
        it should behave like codegenCOORDStoWKB(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like codegenCOORDStoWKB(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ConvertTo Expression from COORDS to WKT" should "do codegen for any index system and any geometry API" in {
        it should behave like codegenCOORDStoWKT(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like codegenCOORDStoWKT(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ConvertTo Expression from COORDS to HEX" should "do codegen for any index system and any geometry API" in {
        it should behave like codegenCOORDStoHEX(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like codegenCOORDStoHEX(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ConvertTo Expression from COORDS to GEOJSON" should "do codegen for any index system and any geometry API" in {
        it should behave like codegenCOORDStoGEOJSON(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like codegenCOORDStoGEOJSON(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ConvertTo Expression from GEOJSON to WKB" should "do codegen for any index system and any geometry API" in {
        it should behave like codegenGEOJSONtoWKB(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like codegenGEOJSONtoWKB(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ConvertTo Expression from GEOJSON to WKT" should "do codegen for any index system and any geometry API" in {
        it should behave like codegenGEOJSONtoWKT(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like codegenGEOJSONtoWKT(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ConvertTo Expression from GEOJSON to HEX" should "do codegen for any index system and any geometry API" in {
        it should behave like codegenGEOJSONtoHEX(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like codegenGEOJSONtoHEX(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ConvertTo Expression from GEOJSON to COORDS" should "do codegen for any index system and any geometry API" in {
        it should behave like codegenGEOJSONtoCOORDS(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like codegenGEOJSONtoCOORDS(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

}
