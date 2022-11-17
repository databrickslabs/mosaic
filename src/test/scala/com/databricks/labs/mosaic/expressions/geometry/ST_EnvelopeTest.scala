package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ST_EnvelopeTest extends MosaicSpatialQueryTest with SharedSparkSession with ST_EnvelopeBehaviors {

    testAllGeometriesCodegen("ST_Envelope behavior") { behavior }
    testAllGeometriesNoCodegen("ST_Envelope behavior") { behavior }
    testAllGeometriesCodegen("ST_Envelope codegen compilation") { codegenCompilation }
    testAllGeometriesNoCodegen("ST_Envelope auxiliary methods") { auxiliaryMethods }

}
