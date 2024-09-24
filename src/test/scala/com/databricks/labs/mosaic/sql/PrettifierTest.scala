package com.databricks.labs.mosaic.sql

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class PrettifierTest extends MosaicSpatialQueryTest with SharedSparkSession with PrettifierBehaviors {

    testAllNoCodegen("Testing prettifier") {
        behavior
    }

}
