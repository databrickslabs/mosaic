package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ST_CentroidTest extends MosaicSpatialQueryTest with SharedSparkSession with ST_CentroidBehaviors {

    testAllGeometriesNoCodegen("ST_Centroid behavior") { centroidBehavior }
    testAllGeometriesCodegen("ST_Centroid codegen behavior") { centroidBehavior }
    testAllGeometriesCodegen("ST_Centroid codegen compilation") { centroidCodegen }
    testAllGeometriesNoCodegen("ST_Centroid auxiliary methods") { auxiliaryMethods }

}
