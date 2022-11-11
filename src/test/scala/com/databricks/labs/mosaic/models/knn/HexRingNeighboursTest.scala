package com.databricks.labs.mosaic.models.knn

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class HexRingNeighboursTest extends MosaicSpatialQueryTest with SharedSparkSession with HexRingNeighboursBehaviors {

    testAllCodegen("HexRingNeighbours leftTransform") { leftTransform }
    testAllCodegen("HexRingNeighbours resultTransform") { resultTransform }
    testAllCodegen("HexRingNeighbours transform") { transform }

}
