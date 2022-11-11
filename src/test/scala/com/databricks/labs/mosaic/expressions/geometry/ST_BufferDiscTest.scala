package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ST_BufferDiscTest extends MosaicSpatialQueryTest with SharedSparkSession with ST_BufferDiscBehaviors {

    testAllGeometriesCodegen("ST_BufferDisc behavior") { behavior }
    testAllGeometriesNoCodegen("ST_BufferDisc behavior") { behavior }
    testAllGeometriesCodegen("ST_BufferDisc codegen compilation") { codegenCompilation }
    testAllGeometriesNoCodegen("ST_BufferDisc auxiliary methods") { auxiliaryMethods }

}
