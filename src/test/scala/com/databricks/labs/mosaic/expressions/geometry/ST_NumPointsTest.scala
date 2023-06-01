package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ST_NumPointsTest extends MosaicSpatialQueryTest with SharedSparkSession with ST_NumPointsBehaviors {

    testAllGeometriesNoCodegen("Testing stNumPoints (no codegen)") { numPointsBehaviour }
    testAllGeometriesCodegen("Testing stNumPoints (codegen)") { numPointsBehaviour }
    testAllGeometriesCodegen("Testing stNumPoints (codegen) compile") { codegenCompilation }
    testAllGeometriesNoCodegen("Testing stNumPoints auxiliaryMethods") { auxiliaryMethods }

}
