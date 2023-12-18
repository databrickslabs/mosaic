package com.databricks.labs.mosaic.codegen

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ConvertToCodegenTest extends MosaicSpatialQueryTest with SharedSparkSession with ConvertToCodegenBehaviors {

    testAllGeometriesCodegen("ConvertTo Expression from WKB to WKB - passthrough") { codegenWKBtoWKB }
    testAllGeometriesCodegen("ConvertTo Expression from WKB to WKT") { codegenWKBtoWKT }
    testAllGeometriesCodegen("ConvertTo Expression from WKB to HEX") { codegenWKBtoHEX }
    testAllGeometriesCodegen("ConvertTo Expression from WKB to COORDS") { codegenWKBtoCOORDS }
    testAllGeometriesCodegen("ConvertTo Expression from WKB to GEOJSON") { codegenWKBtoGEOJSON }
    testAllGeometriesCodegen("ConvertTo Expression from WKT to WKB") { codegenWKTtoWKB }
    testAllGeometriesCodegen("ConvertTo Expression from WKT to HEX") { codegenWKTtoHEX }
    testAllGeometriesCodegen("ConvertTo Expression from WKT to COORDS") { codegenWKTtoCOORDS }
    testAllGeometriesCodegen("ConvertTo Expression from WKT to GEOJSON") { codegenWKTtoGEOJSON }
    testAllGeometriesCodegen("ConvertTo Expression from HEX to WKB") { codegenHEXtoWKB }
    testAllGeometriesCodegen("ConvertTo Expression from HEX to WKT") { codegenHEXtoWKT }
    testAllGeometriesCodegen("ConvertTo Expression from HEX to COORDS") { codegenHEXtoCOORDS }
    testAllGeometriesCodegen("ConvertTo Expression from HEX to GEOJSON") { codegenHEXtoGEOJSON }
    testAllGeometriesCodegen("ConvertTo Expression from COORDS to WKB") { codegenCOORDStoWKB }
    testAllGeometriesCodegen("ConvertTo Expression from COORDS to WKT") { codegenCOORDStoWKT }
    testAllGeometriesCodegen("ConvertTo Expression from COORDS to HEX") { codegenCOORDStoHEX }
    testAllGeometriesCodegen("ConvertTo Expression from COORDS to GEOJSON") { codegenCOORDStoGEOJSON }
    testAllGeometriesCodegen("ConvertTo Expression from GEOJSON to WKB") { codegenGEOJSONtoWKB }
    testAllGeometriesCodegen("ConvertTo Expression from GEOJSON to WKT") { codegenGEOJSONtoWKT }
    testAllGeometriesCodegen("ConvertTo Expression from GEOJSON to HEX") { codegenGEOJSONtoHEX }
    testAllGeometriesCodegen("ConvertTo Expression from GEOJSON to COORDS") { codegenGEOJSONtoCOORDS }

}
