package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class MosaicFillTest extends MosaicSpatialQueryTest with SharedSparkSession with MosaicFillBehaviors {

    testAllNoCodegen("MosaicFill WKT") { wktMosaicFill }
    testAllNoCodegen("MosaicFill WKB") { wkbMosaicFill }
    testAllNoCodegen("MosaicFill Hex") { hexMosaicFill }
    testAllNoCodegen("MosaicFill Coords") { coordsMosaicFill }
    testAllNoCodegen("MosaicFill points") { mosaicFillPoints }
    testAllNoCodegen("MosaicFill multi points") { mosaicFillMultiPoints }
    testAllNoCodegen("MosaicFill column function signatures") { columnFunctionSignatures }
    testAllNoCodegen("MosaicFill auxiliary methods") { auxiliaryMethods }
    testAllNoCodegen("MosaicFill with H3 cells crossing projected icosahedron edges  (#260)") {
        wktMosaicTessellate
    }

}
