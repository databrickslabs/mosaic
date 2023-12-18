package com.databricks.labs.mosaic.sql

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.raster.api.RasterAPI.GDAL
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkSuite
import org.scalatest.flatspec.AnyFlatSpec

class TestMosaicFrame extends AnyFlatSpec with MosaicFrameBehaviors with SparkSuite {

    "MosaicFrame" should "be instantiated from points using any index system and any geometry API" in {
        it should behave like testConstructFromPoints(spark, MosaicContext.build(H3IndexSystem, ESRI, GDAL))
        it should behave like testConstructFromPoints(spark, MosaicContext.build(H3IndexSystem, JTS, GDAL))
        it should behave like testConstructFromPoints(spark, MosaicContext.build(BNGIndexSystem, ESRI, GDAL))
        it should behave like testConstructFromPoints(spark, MosaicContext.build(BNGIndexSystem, JTS, GDAL))
    }
    "MosaicFrame" should "be instantiated from polygons using any index system and any geometry API" in {
        it should behave like testConstructFromPolygons(spark, MosaicContext.build(H3IndexSystem, ESRI, GDAL))
        it should behave like testConstructFromPolygons(spark, MosaicContext.build(H3IndexSystem, JTS, GDAL))
        it should behave like testConstructFromPolygons(spark, MosaicContext.build(BNGIndexSystem, ESRI, GDAL))
        it should behave like testConstructFromPolygons(spark, MosaicContext.build(BNGIndexSystem, JTS, GDAL))
    }
    "MosaicFrame" should "apply an index system to point geometries" in {
        it should behave like testIndexPoints(spark, MosaicContext.build(H3IndexSystem, ESRI, GDAL), 9)
        it should behave like testIndexPoints(spark, MosaicContext.build(H3IndexSystem, JTS, GDAL), 9)
        it should behave like testIndexPoints(spark, MosaicContext.build(BNGIndexSystem, ESRI, GDAL), 4)
        it should behave like testIndexPoints(spark, MosaicContext.build(BNGIndexSystem, JTS, GDAL), 4)
    }
    "MosaicFrame" should "apply an index system to polygon geometries" in {
        it should behave like testIndexPolygons(spark, MosaicContext.build(H3IndexSystem, ESRI, GDAL), 9)
        it should behave like testIndexPolygons(spark, MosaicContext.build(H3IndexSystem, JTS, GDAL), 9)
        it should behave like testIndexPolygons(spark, MosaicContext.build(BNGIndexSystem, ESRI, GDAL), 3)
        it should behave like testIndexPolygons(spark, MosaicContext.build(BNGIndexSystem, JTS, GDAL), 3)
    }
    "MosaicFrame" should "apply an index system to polygon geometries and explode into the Mosaic column structure" in {
        it should behave like testIndexPolygonsExplode(spark, MosaicContext.build(H3IndexSystem, ESRI, GDAL), 9)
        it should behave like testIndexPolygonsExplode(spark, MosaicContext.build(H3IndexSystem, JTS, GDAL), 9)
        it should behave like testIndexPolygonsExplode(spark, MosaicContext.build(BNGIndexSystem, ESRI, GDAL), 3)
        it should behave like testIndexPolygonsExplode(spark, MosaicContext.build(BNGIndexSystem, JTS, GDAL), 3)
    }
    "MosaicFrame" should "suggest an appropriate resolution to index a set of polygon geometries" in {
        it should behave like testGetOptimalResolution(spark, MosaicContext.build(H3IndexSystem, ESRI, GDAL), 3, 9)
        it should behave like testGetOptimalResolution(spark, MosaicContext.build(H3IndexSystem, JTS, GDAL), 3, 9)
        it should behave like testGetOptimalResolution(spark, MosaicContext.build(BNGIndexSystem, ESRI, GDAL), 2, -4)
        it should behave like testGetOptimalResolution(spark, MosaicContext.build(BNGIndexSystem, JTS, GDAL), 2, -4)
    }
    "MosaicFrame" should "allow users to generate indexes at a number of different resolutions" in {
        it should behave like testMultiplePointIndexResolutions(spark, MosaicContext.build(H3IndexSystem, ESRI, GDAL), 6, 10)
        it should behave like testMultiplePointIndexResolutions(spark, MosaicContext.build(H3IndexSystem, JTS, GDAL), 6, 10)
        it should behave like testMultiplePointIndexResolutions(spark, MosaicContext.build(BNGIndexSystem, ESRI, GDAL), 3, 6)
        it should behave like testMultiplePointIndexResolutions(spark, MosaicContext.build(BNGIndexSystem, JTS, GDAL), 3, 6)
    }
    "MosaicFrame" should "join point and polygon typed MosaicFrames" in {
        it should behave like testPointInPolyJoin(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark, 9)
        it should behave like testPointInPolyJoin(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark, 9)
        it should behave like testPointInPolyJoin(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark, 3)
        it should behave like testPointInPolyJoin(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark, 3)
    }
    "MosaicFrame" should "join point and polygon typed MosaicFrames when the polygons are exploded" in {
        it should behave like testPointInPolyJoinExploded(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark, 9)
        it should behave like testPointInPolyJoinExploded(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark, 9)
        it should behave like testPointInPolyJoinExploded(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark, 4)
        it should behave like testPointInPolyJoinExploded(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark, 4)
    }
    "MosaicFrame" should "join point and polygon typed MosaicFrames when the points are incorrectly indexed" in {
        it should behave like testPoorlyConfiguredPointInPolyJoins(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark, 9)
        it should behave like testPoorlyConfiguredPointInPolyJoins(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark, 9)
        it should behave like testPoorlyConfiguredPointInPolyJoins(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark, 3)
        it should behave like testPoorlyConfiguredPointInPolyJoins(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark, 3)
    }
    "MosaicFrame" should "be prettified without exceptions" in {
        it should behave like testPrettifier(spark, MosaicContext.build(H3IndexSystem, ESRI, GDAL), 9)
        it should behave like testPrettifier(spark, MosaicContext.build(H3IndexSystem, JTS, GDAL), 9)
        it should behave like testPrettifier(spark, MosaicContext.build(BNGIndexSystem, ESRI, GDAL), 4)
        it should behave like testPrettifier(spark, MosaicContext.build(BNGIndexSystem, JTS, GDAL), 4)
    }
    "MosaicFrame" should "throw expected exceptions" in {
        it should behave like testExceptions(spark, MosaicContext.build(H3IndexSystem, ESRI, GDAL), 32)
        it should behave like testExceptions(spark, MosaicContext.build(H3IndexSystem, JTS, GDAL), 32)
        it should behave like testExceptions(spark, MosaicContext.build(BNGIndexSystem, ESRI, GDAL), 32)
        it should behave like testExceptions(spark, MosaicContext.build(BNGIndexSystem, JTS, GDAL), 32)
    }

}
