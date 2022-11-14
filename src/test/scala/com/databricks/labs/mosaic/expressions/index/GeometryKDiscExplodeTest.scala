package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class GeometryKDiscExplodeTest extends MosaicSpatialQueryTest with SharedSparkSession with GeometryKDiscExplodeBehaviors {

    testAllNoCodegen("GeometryKDiscExplode behavior on computed columns") { behavior }
    testAllNoCodegen("GeometryKDiscExplode column function signatures") { columnFunctionSignatures }
    testAllNoCodegen("GeometryKDiscExplode auxiliary methods") { auxiliaryMethods }

}
