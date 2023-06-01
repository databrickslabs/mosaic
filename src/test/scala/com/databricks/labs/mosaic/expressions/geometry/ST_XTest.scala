package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ST_XTest extends MosaicSpatialQueryTest with SharedSparkSession with ST_XBehaviors {

    testAllGeometriesNoCodegen("Testing stX NO_CODEGEN") { stxBehavior }
    testAllGeometriesCodegen("Testing stX CODEGEN") { stxBehavior }
    testAllGeometriesCodegen("Testing stX CODEGEN compilation") { stxCodegen }
    testAllGeometriesNoCodegen("Testing stX auxiliary methods") { auxiliaryMethods }

}
