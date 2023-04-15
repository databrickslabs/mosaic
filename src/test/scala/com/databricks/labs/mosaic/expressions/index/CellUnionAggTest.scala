package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class CellUnionAggTest extends MosaicSpatialQueryTest with SharedSparkSession with CellUnionAggBehaviors {

    testAllNoCodegen("CellUnionAgg behavior on computed columns") { behaviorComputedColumns }
    testAllNoCodegen("CellUnionAgg column function signatures") {
        columnFunctionSignatures
    }
    testAllNoCodegen("CellUnionAgg auxiliary methods") {
        auxiliaryMethods
    }

}
