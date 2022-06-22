package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkSuite
import org.scalatest.flatspec.AnyFlatSpec

class TestGeometryProcessors extends AnyFlatSpec with GeometryProcessorsBehaviors with SparkSuite {

    "ST_Perimeter and ST_Length" should "compute the total length for any index system and any geometry API" in {
        it should behave like lengthCalculation(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like lengthCalculation(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like lengthCalculation(MosaicContext.build(BNGIndexSystem, ESRI), spark)
        it should behave like lengthCalculation(MosaicContext.build(BNGIndexSystem, JTS), spark)
    }

    "ST_Area" should "compute the area for any index system and any geometry API" in {
        it should behave like areaCalculation(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like areaCalculation(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like areaCalculation(MosaicContext.build(BNGIndexSystem, ESRI), spark)
        it should behave like areaCalculation(MosaicContext.build(BNGIndexSystem, JTS), spark)
    }

    "ST_Centroid2D" should "compute the centroid2D for any index system and any geometry API" in {
        it should behave like centroid2DCalculation(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like centroid2DCalculation(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like centroid2DCalculation(MosaicContext.build(BNGIndexSystem, ESRI), spark)
        it should behave like centroid2DCalculation(MosaicContext.build(BNGIndexSystem, JTS), spark)
    }

    "ST_Distance" should "compute the distance for any index system and any geometry API" in {
        it should behave like distanceCalculation(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like distanceCalculation(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like distanceCalculation(MosaicContext.build(BNGIndexSystem, ESRI), spark)
        it should behave like distanceCalculation(MosaicContext.build(BNGIndexSystem, JTS), spark)
    }

    "ST_Contains" should "compute the contains relationship for any index system and any geometry API" in {
        it should behave like polygonContains(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like polygonContains(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like polygonContains(MosaicContext.build(BNGIndexSystem, ESRI), spark)
        it should behave like polygonContains(MosaicContext.build(BNGIndexSystem, JTS), spark)
    }

    "ST_ConvexHull" should "compute the convex hull for any index system and any geometry API" in {
        it should behave like convexHullGeneration(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like convexHullGeneration(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like convexHullGeneration(MosaicContext.build(BNGIndexSystem, ESRI), spark)
        it should behave like convexHullGeneration(MosaicContext.build(BNGIndexSystem, JTS), spark)
    }

    "ST_Buffer" should "compute the buffer geometry for any geometry API" in {
        it should behave like bufferCalculation(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like bufferCalculation(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like bufferCalculation(MosaicContext.build(BNGIndexSystem, ESRI), spark)
        it should behave like bufferCalculation(MosaicContext.build(BNGIndexSystem, JTS), spark)
    }

    "Geometry Processors" should "implement auxiliary methods correctly" in {
        it should behave like auxiliaryMethods(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like auxiliaryMethods(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like auxiliaryMethods(MosaicContext.build(BNGIndexSystem, ESRI), spark)
        it should behave like auxiliaryMethods(MosaicContext.build(BNGIndexSystem, JTS), spark)
    }

}
