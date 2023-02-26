package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ST_YTest extends MosaicSpatialQueryTest with SharedSparkSession with ST_YBehaviors {

    testAllNoCodegen("Testing ST_Y NO_CODEGEN") { styBehavior }
    testAllCodegen("Testing ST_Y CODEGEN") { styBehavior }
    testAllCodegen("Testing ST_Y CODEGEN compilation") { styCodegen }
    testAllNoCodegen("Testing auxiliary methods") { auxiliaryMethods }

}
