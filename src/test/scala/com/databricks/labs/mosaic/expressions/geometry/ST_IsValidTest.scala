package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ST_IsValidTest extends MosaicSpatialQueryTest with SharedSparkSession with ST_IsValidBehaviors {

    testAllGeometriesNoCodegen("Testing stIsValid NO_CODEGEN") { isValidBehaviour }
    testAllGeometriesCodegen("Testing stIsValid CODEGEN compilation") { isValidCodegen }
    testAllGeometriesCodegen("Testing stIsValid CODEGEN") { isValidBehaviour }
    testAllGeometriesNoCodegen("Testing auxiliary methods") { auxiliaryMethods }

}
