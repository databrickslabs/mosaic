package com.databricks.labs.mosaic.models

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ApproximateSpatialKNNTest extends MosaicSpatialQueryTest with SharedSparkSession with ApproximateSpatialKNNBehaviors {

    testAllCodegen("ApproximateSpatialKNN behavior") { behavior }

}
