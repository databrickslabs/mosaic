package com.databricks.mosaic.expressions.geometry

import org.scalatest.flatspec.AnyFlatSpec

import org.apache.spark.sql.SparkSession

import com.databricks.mosaic.core.geometry.api.GeometryAPI.{JTS, OGC}
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.test.{SparkCodeGenSuite, SparkSuite}

class TestGeometryProcessorsCodegen extends AnyFlatSpec with GeometryProcessorsBehaviors with SparkCodeGenSuite {

    "ST_Perimeter and ST_Length" should "compute the total length for any index system and any geometry API" in {
        it should behave like lengthCalculationCodegen(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like lengthCalculationCodegen(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like lengthCalculation(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like lengthCalculation(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ST_Area" should "compute the area for any index system and any geometry API" in {
        it should behave like areaCodegen(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like areaCodegen(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like areaCalculation(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like areaCalculation(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ST_Centroid2D" should "compute the centroid2D for any index system and any geometry API" in {
        it should behave like centroid2DCodegen(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like centroid2DCodegen(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like centroid2DCalculation(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like centroid2DCalculation(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ST_Distance" should "compute the distance for any index system and any geometry API" in {
        it should behave like distanceCodegen(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like distanceCodegen(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like distanceCalculation(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like distanceCalculation(MosaicContext.build(H3IndexSystem, JTS), spark)
    }


    "ST_Contains" should "compute the contains relationship for any index system and any geometry API" in {
        it should behave like containsCodegen(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like containsCodegen(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like polygonContains(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like polygonContains(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ST_ConvexHull" should "compute the convex hull for any index system and any geometry API" in {
        it should behave like convexHullCodegen(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like convexHullCodegen(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like convexHullGeneration(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like convexHullGeneration(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ST_ transformations" should "execute without errors for any index system and any geometry API" in {
        it should behave like transformationsCodegen(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like transformationsCodegen(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

}
