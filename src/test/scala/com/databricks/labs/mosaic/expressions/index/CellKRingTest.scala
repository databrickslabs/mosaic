package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class CellKRingTest extends MosaicSpatialQueryTest with SharedSparkSession with CellKRingBehaviors {

    testAllNoCodegen("CellKRing behavior on computed columns") { behaviorComputedColumns }
    testAllNoCodegen("CellKRing column function signatures") { columnFunctionSignatures }
    testAllNoCodegen("CellKRing auxiliary methods") { auxiliaryMethods }

}
