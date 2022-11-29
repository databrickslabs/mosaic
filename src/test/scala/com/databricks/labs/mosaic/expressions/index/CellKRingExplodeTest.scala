package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class CellKRingExplodeTest extends MosaicSpatialQueryTest with SharedSparkSession with CellKRingExplodeBehaviors {

    testAllNoCodegen("CellKRingExplode behavior on computed columns") { behaviorComputedColumns }
    testAllNoCodegen("CellKRingExplode column function signatures") { columnFunctionSignatures }
    testAllNoCodegen("CellKRingExplode auxiliary methods") { auxiliaryMethods }

}
