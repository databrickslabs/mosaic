package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ST_GeometryTypeTest extends MosaicSpatialQueryTest with SharedSparkSession with ST_GeometryTypeBehaviors {

    testAllGeometriesNoCodegen("Testing stGeometryType wkt") { wktTypesBehavior }
    testAllGeometriesNoCodegen("Testing stGeometryType hex") { hexTypesBehavior }
    testAllGeometriesCodegen("Testing stGeometryType CODEGEN compilation wkt") { wktTypesCodegen }
    testAllGeometriesCodegen("Testing stGeometryType CODEGEN compilation hex") { hexTypesCodegen }
    testAllGeometriesCodegen("Testing stGeometryType CODEGEN wkt") { wktTypesBehavior }
    testAllGeometriesCodegen("Testing stGeometryType CODEGEN hex") { hexTypesBehavior }
    testAllGeometriesNoCodegen("Testing stGeometryType auxiliaryMethods") { auxiliaryMethods }

}
