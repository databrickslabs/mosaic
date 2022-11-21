package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class GeometryKLoopExplodeTest extends MosaicSpatialQueryTest with SharedSparkSession with GeometryKLoopExplodeBehaviors {

    testAllNoCodegen("GeometryKLoopExplode behavior on computed columns") { behavior }
    testAllNoCodegen("GeometryKLoopExplode column function signatures") { columnFunctionSignatures }
    testAllNoCodegen("GeometryKLoopExplode auxiliary methods") { auxiliaryMethods }

}
