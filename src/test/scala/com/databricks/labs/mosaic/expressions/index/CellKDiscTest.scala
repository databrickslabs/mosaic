package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class CellKDiscTest extends MosaicSpatialQueryTest with SharedSparkSession with CellKDiscBehaviors {

    testAllNoCodegen("CellKDisc behavior on computed columns") { behaviorComputedColumns }
    testAllNoCodegen("CellKDisc column function signatures") { columnFunctionSignatures }
    testAllNoCodegen("CellKDisc auxiliary methods") { auxiliaryMethods }

}
