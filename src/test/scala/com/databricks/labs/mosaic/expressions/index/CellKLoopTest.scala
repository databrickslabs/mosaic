package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class CellKLoopTest extends MosaicSpatialQueryTest with SharedSparkSession with CellKLoopBehaviors {

    testAllNoCodegen("CellKLoop behavior on computed columns") { behaviorComputedColumns }
    testAllNoCodegen("CellKLoop column function signatures") { columnFunctionSignatures }
    testAllNoCodegen("CellKLoop auxiliary methods") { auxiliaryMethods }

}
