package com.databricks.labs.mosaic.sql

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkSuite
import org.scalatest.flatspec.AnyFlatSpec

class TestMosaicFrame extends AnyFlatSpec with MosaicFrameBehaviors with SparkSuite {

    "MosaicFrame" should "be instantiated from points using any index system and any geometry API" in {
        it should behave like testConstructFromPoints(spark)
    }
    "MosaicFrame" should "be instantiated from polygons using any index system and any geometry API" in {
        it should behave like testConstructFromPolygons(spark)
    }
    "MosaicFrame" should "apply an index system to point geometries" in {
        it should behave like testIndexPoints(spark)
    }
    "MosaicFrame" should "apply an index system to polygon geometries" in {
        it should behave like testIndexPolygons(spark)
    }
    "MosaicFrame" should "apply an index system to polygon geometries and explode into the Mosaic column structure" in {
        it should behave like testIndexPolygonsExplode(spark)
    }
    "MosaicFrame" should "suggest an appropriate resolution to index a set of polygon geometries" in {
        it should behave like testGetOptimalResolution(spark)
    }
    "MosaicFrame" should "allow users to generate indexes at a number of different resolutions" in {
        it should behave like testMultiplePointIndexResolutions(spark)
    }
    "MosaicFrame" should "join point and polygon typed MosaicFrames" in {
        it should behave like testPointInPolyJoin(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like testPointInPolyJoin(MosaicContext.build(H3IndexSystem, JTS), spark)
    }
    "MosaicFrame" should "join point and polygon typed MosaicFrames when the polygons are exploded" in {
        it should behave like testPointInPolyJoinExploded(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like testPointInPolyJoinExploded(MosaicContext.build(H3IndexSystem, JTS), spark)
    }
    "MosaicFrame" should "join point and polygon typed MosaicFrames when the points are incorrectly indexed" in {
        it should behave like testPoorlyConfiguredPointInPolyJoins(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like testPoorlyConfiguredPointInPolyJoins(MosaicContext.build(H3IndexSystem, JTS), spark)
    }
    "MosaicFrame" should "be prettified without exceptions" in {
        it should behave like testPrettifier(spark)
    }
    "MosaicFrame" should "throw expected exceptions" in {
        it should behave like testExceptions(spark)
    }

}
