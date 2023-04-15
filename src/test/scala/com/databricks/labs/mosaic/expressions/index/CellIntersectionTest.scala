package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class CellIntersectionTest extends MosaicSpatialQueryTest with SharedSparkSession with CellIntersectionBehaviors {
    testAllNoCodegen("CellIntersection behavior on computed columns") { behaviorComputedColumns }
    testAllNoCodegen("CellIntersection column function signatures") {
        columnFunctionSignatures
    }
    testAllNoCodegen("CellIntersection auxiliary methods") {
        auxiliaryMethods
    }
}
