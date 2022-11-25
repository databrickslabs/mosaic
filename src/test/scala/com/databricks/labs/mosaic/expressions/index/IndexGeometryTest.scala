package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class IndexGeometryTest extends MosaicSpatialQueryTest with SharedSparkSession with IndexGeometryBehaviors {

    testAllNoCodegen("IndexGeometry auxiliaryMethods") { auxiliaryMethods }

}
