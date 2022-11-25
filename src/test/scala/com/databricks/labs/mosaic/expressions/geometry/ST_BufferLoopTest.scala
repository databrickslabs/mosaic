package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ST_BufferLoopTest extends MosaicSpatialQueryTest with SharedSparkSession with ST_BufferLoopBehaviors {

    testAllGeometriesCodegen("ST_BufferLoop behavior") { behavior }
    testAllGeometriesNoCodegen("ST_BufferLoop behavior") { behavior }
    testAllGeometriesCodegen("ST_BufferLoop codegen compilation") { codegenCompilation }
    testAllGeometriesNoCodegen("ST_BufferLoop auxiliary methods") { auxiliaryMethods }

}
