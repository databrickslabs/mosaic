package com.databricks.labs.mosaic.sql

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class AnalyzerTest extends MosaicSpatialQueryTest with SharedSparkSession with AnalyzerBehaviors {

    testAllNoCodegen("Testing analyzer") { behavior }

}
