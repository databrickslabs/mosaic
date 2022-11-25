package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class GeometryKRingExplodeTest extends MosaicSpatialQueryTest with SharedSparkSession with GeometryKRingExplodeBehaviors {

    testAllNoCodegen("GeometryKRingExplode behavior") { behavior }
    testAllNoCodegen("GeometryKRingExplode column function signatures") { columnFunctionSignatures }
    testAllNoCodegen("GeometryKRingExplode auxiliary methods") { auxiliaryMethods }

}
