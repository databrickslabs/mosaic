package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ST_AsGeoJSONTileAggTest extends MosaicSpatialQueryTest with SharedSparkSession with ST_AsGeoJSONTileAggBehaviors {

    testAllNoCodegen("Testing stAsGeoJSONTileAgg") { behavior }

}
