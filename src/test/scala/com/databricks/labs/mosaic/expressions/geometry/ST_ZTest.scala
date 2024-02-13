package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ST_ZTest extends MosaicSpatialQueryTest with SharedSparkSession with ST_ZBehaviors {

    testAllGeometriesNoCodegen("Testing stZ NO_CODEGEN") { stzBehavior }
    testAllGeometriesCodegen("Testing stZ CODEGEN") { stzBehavior }
    testAllGeometriesCodegen("Testing stZ CODEGEN compilation") { stzCodegen }
    testAllGeometriesNoCodegen("Testing stZ auxiliary methods") { auxiliaryMethods }

}
