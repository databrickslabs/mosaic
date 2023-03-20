package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class GridDistanceTest extends MosaicSpatialQueryTest with SharedSparkSession with GridDistanceBehaviors {

    testAllNoCodegen("GridDistance behavior on computed columns") { behaviorGridDistance }
    testAllNoCodegen("GridDistance auxiliary methods") { auxiliaryMethods }

}
