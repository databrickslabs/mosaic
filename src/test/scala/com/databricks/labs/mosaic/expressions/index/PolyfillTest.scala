package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class PolyfillTest extends MosaicSpatialQueryTest with SharedSparkSession with PolyfillBehaviors {

    testAllNoCodegen("Polyfill on computed columns") { polyfillOnComputedColumns }
    testAllNoCodegen("Polyfill WKT") { wktPolyfill }
    testAllNoCodegen("Polyfill WKB") { wkbPolyfill }
    testAllNoCodegen("Polyfill Hex") { hexPolyfill }
    testAllNoCodegen("Polyfill Coords") { coordsPolyfill }
    testAllNoCodegen("Polyfill column function signatures") { columnFunctionSignatures }
    testAllNoCodegen("Polyfill auxiliary methods") { auxiliaryMethods }

}
