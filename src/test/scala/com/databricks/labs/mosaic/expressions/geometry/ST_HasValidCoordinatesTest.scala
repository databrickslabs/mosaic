package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ST_HasValidCoordinatesTest extends MosaicSpatialQueryTest with SharedSparkSession with ST_HasValidCoordinatesBehaviors {

    testAllGeometriesNoCodegen("ST_HasValidCoordinates behavior") { hasValidCoordinatesBehaviours }
    testAllGeometriesCodegen("ST_HasValidCoordinates codegen compilation") { expressionCodegen }
    testAllGeometriesCodegen("ST_HasValidCoordinates codegen behavior") { hasValidCoordinatesBehaviours }
    testAllGeometriesNoCodegen("ST_HasValidCoordinates auxiliary methods") { auxiliaryMethods }

}
