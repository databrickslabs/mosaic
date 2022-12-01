package com.databricks.labs.mosaic.models.knn

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class GridRingNeighboursTest extends MosaicSpatialQueryTest with SharedSparkSession with GridRingNeighboursBehaviors {

    testAllCodegen("GridRingNeighbours leftTransform") { leftTransform }
    testAllCodegen("GridRingNeighbours resultTransform") { resultTransform }
    testAllCodegen("GridRingNeighbours transform") { transform }

}
