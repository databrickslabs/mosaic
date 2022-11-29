package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class GeometryKLoopTest extends MosaicSpatialQueryTest with SharedSparkSession with GeometryKLoopBehaviors {

    testAllNoCodegen("GeometryKLoop behavior on computed columns") { behavior }
    testAllNoCodegen("GeometryKLoop column function signatures") { columnFunctionSignatures }
    testAllNoCodegen("GeometryKLoop auxiliary methods") { auxiliaryMethods }

}
