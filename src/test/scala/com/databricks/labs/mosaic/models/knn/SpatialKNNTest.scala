package com.databricks.labs.mosaic.models.knn

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class SpatialKNNTest extends MosaicSpatialQueryTest with SharedSparkSession with SpatialKNNBehaviors {

    testAllCodegen("SpatialKNN behavior") { behavior }
    //testAllCodegen("SpatialKNN behavior with approximation") { behaviorApproximate }

}
