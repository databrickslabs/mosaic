package com.databricks.labs.mosaic.sql

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class TestMosaicFrame extends MosaicSpatialQueryTest with SharedSparkSession with MosaicFrameBehaviors {

    testAllCodegen("MosaicFrame be instantiated from points") { testConstructFromPoints }
    testAllCodegen("MosaicFrame be instantiated from polygons") { testConstructFromPolygons }
    testAllCodegen("MosaicFrame apply an index system to point geometries") { testIndexPoints }
    testAllCodegen("MosaicFrame apply an index system to polygon geometries") { testIndexPolygons }
    testAllCodegen("MosaicFrame apply an index system to polygon geometries and explode") { testIndexPolygonsExplode }
    testAllCodegen("MosaicFrame get optimal resolution") { testGetOptimalResolution }
    testAllCodegen("MosaicFrame allow users to generate indexes at a number of different resolutions") { testMultiplePointIndexResolutions }
    testAllCodegen("MosaicFrame join point and polygon typed MosaicFrames") { testPointInPolyJoin }
    testAllCodegen("MosaicFrame join point and polygon typed MosaicFrames when the polygons are exploded") { testPointInPolyJoinExploded }
    testAllCodegen("MosaicFrame join point and polygon typed MosaicFrames when the points are incorrectly indexed") {
        testPoorlyConfiguredPointInPolyJoins
    }
    testAllCodegen("MosaicFrame be prettified without exceptions") { testPrettifier }
    testAllCodegen("MosaicFrame throw expected exceptions") { testExceptions }

}
