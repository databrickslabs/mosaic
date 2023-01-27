package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class PointIndexTest extends MosaicSpatialQueryTest with SharedSparkSession with PointIndexBehaviors {

    testAllNoCodegen("PointIndex behavior int resolution") { behaviorInt }
    testAllNoCodegen("PointIndex behavior string resolution") { behaviorString }
    testAllNoCodegen("PointIndex auxiliary methods") { auxiliaryMethods }

}
