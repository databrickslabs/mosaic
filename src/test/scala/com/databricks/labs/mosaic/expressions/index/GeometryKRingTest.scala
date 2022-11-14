package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class GeometryKRingTest extends MosaicSpatialQueryTest with SharedSparkSession with GeometryKRingBehaviors {

    testAllNoCodegen("GeometryKRing behavior on computed columns") { behavior }
    testAllNoCodegen("GeometryKRing column function signatures") { columnFunctionSignatures }
    testAllNoCodegen("GeometryKRing auxiliary methods") { auxiliaryMethods }

}
