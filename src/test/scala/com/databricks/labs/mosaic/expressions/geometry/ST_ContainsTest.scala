package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ST_ContainsTest extends MosaicSpatialQueryTest with SharedSparkSession with ST_ContainsBehaviors {

    testAllGeometriesNoCodegen("ST_Contains behavior") { containsBehavior }
    testAllGeometriesCodegen("ST_Contains codegen compilation") { containsCodegen }
    testAllGeometriesCodegen("ST_Contains codegen behavior") { containsBehavior }
    testAllGeometriesNoCodegen("ST_Contains auxiliary methods") { auxiliaryMethods }

}
