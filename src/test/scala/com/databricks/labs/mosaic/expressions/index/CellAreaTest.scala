package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class CellAreaTest extends MosaicSpatialQueryTest with SharedSparkSession with CellAreaBehaviors {

    testAllNoCodegen("CellArea behavior on computed columns") { behaviorComputedColumns }
    testAllNoCodegen("CellArea column function signatures") { columnFunctionSignatures }
    testAllNoCodegen("Cellarea auxiliary methods") { auxiliaryMethods }

}
