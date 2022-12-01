package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class CellKLoopExplodeTest extends MosaicSpatialQueryTest with SharedSparkSession with CellKLoopExplodeBehaviors {

    testAllNoCodegen("CellKLoopExplode behavior on computed columns") { behaviorComputedColumns }
    testAllNoCodegen("CellKLoopExplode column function signatures") { columnFunctionSignatures }
    testAllNoCodegen("CellKLoopExplode auxiliary methods") { auxiliaryMethods }

}
