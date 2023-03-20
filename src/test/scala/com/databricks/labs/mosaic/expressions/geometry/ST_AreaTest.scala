package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ST_AreaTest extends MosaicSpatialQueryTest with SharedSparkSession with ST_AreaBehaviors {

    testAllGeometriesNoCodegen("Testing stArea") { areaBehavior }
    testAllGeometriesCodegen("Testing stArea CODEGEN compilation") { areaCodegen }
    testAllGeometriesCodegen("Testing stArea CODEGEN") { areaBehavior }
    testAllGeometriesNoCodegen("Testing stArea auxiliaryMethods") { auxiliaryMethods }

}
