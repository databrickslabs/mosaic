package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class CellUnionTest extends MosaicSpatialQueryTest with SharedSparkSession with CellUnionBehaviors {

    testAllNoCodegen("CellUnion behavior on computed columns") { behaviorComputedColumns }
    testAllNoCodegen("CellUnion column function signatures") {
        columnFunctionSignatures
    }
    testAllNoCodegen("CellUnion auxiliary methods") {
        auxiliaryMethods
    }

}
