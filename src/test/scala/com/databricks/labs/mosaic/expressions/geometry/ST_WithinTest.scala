package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ST_WithinTest extends MosaicSpatialQueryTest with SharedSparkSession with ST_WithinBehaviors {

    testAllGeometriesNoCodegen("ST_Within behavior") { withinBehavior }
    testAllGeometriesCodegen("ST_Within codegen compilation") { withinCodegen }
    testAllGeometriesCodegen("ST_Within codegen behavior") { withinBehavior }
    testAllGeometriesNoCodegen("ST_Within auxiliary methods") { auxiliaryMethods }

}
