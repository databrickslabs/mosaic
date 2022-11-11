package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class CellKDiscExplodeTest extends MosaicSpatialQueryTest with SharedSparkSession with CellKDiscExplodeBehaviors {

    testAllNoCodegen("CellKDiscExplode behavior on computed columns") { behaviorComputedColumns }
    testAllNoCodegen("CellKDiscExplode auxiliary methods") { auxiliaryMethods }

}
