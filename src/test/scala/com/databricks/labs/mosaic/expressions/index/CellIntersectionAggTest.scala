package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class CellIntersectionAggTest extends MosaicSpatialQueryTest with SharedSparkSession with CellIntersectionAggBehaviors {

    testAllNoCodegen("CellIntersectionAgg behavior on computed columns") { behaviorComputedColumns }
    testAllNoCodegen("CellIntersectionAgg column function signatures") {
        columnFunctionSignatures
    }
    testAllNoCodegen("CellIntersectionAgg auxiliary methods") {
        auxiliaryMethods
    }

}
