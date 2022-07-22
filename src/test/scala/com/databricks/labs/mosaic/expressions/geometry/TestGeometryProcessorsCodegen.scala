package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.raster.api.RasterAPI.GDAL
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkCodeGenSuite
import org.scalatest.flatspec.AnyFlatSpec

class TestGeometryProcessorsCodegen extends AnyFlatSpec with GeometryProcessorsBehaviors with SparkCodeGenSuite {

    "ST_Perimeter and ST_Length" should "compute the total length for any index system and any geometry API" in {
        it should behave like lengthCalculationCodegen(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like lengthCalculationCodegen(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like lengthCalculation(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like lengthCalculation(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like lengthCalculationCodegen(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like lengthCalculationCodegen(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
        it should behave like lengthCalculation(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like lengthCalculation(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
    }

    "ST_Area" should "compute the area for any index system and any geometry API" in {
        it should behave like areaCodegen(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like areaCodegen(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like areaCalculation(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like areaCalculation(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like areaCodegen(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like areaCodegen(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
        it should behave like areaCalculation(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like areaCalculation(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
    }

    "ST_Centroid2D" should "compute the centroid2D for any index system and any geometry API" in {
        it should behave like centroid2DCodegen(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like centroid2DCodegen(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like centroid2DCalculation(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like centroid2DCalculation(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like centroid2DCodegen(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like centroid2DCodegen(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
        it should behave like centroid2DCalculation(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like centroid2DCalculation(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
    }

    "ST_Distance" should "compute the distance for any index system and any geometry API" in {
        it should behave like distanceCodegen(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like distanceCodegen(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like distanceCalculation(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like distanceCalculation(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like distanceCodegen(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like distanceCodegen(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
        it should behave like distanceCalculation(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like distanceCalculation(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
    }

    "ST_Contains" should "compute the contains relationship for any index system and any geometry API" in {
        it should behave like containsCodegen(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like containsCodegen(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like polygonContains(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like polygonContains(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like containsCodegen(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like containsCodegen(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
        it should behave like polygonContains(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like polygonContains(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
    }

    "ST_ConvexHull" should "compute the convex hull for any index system and any geometry API" in {
        it should behave like convexHullCodegen(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like convexHullCodegen(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like convexHullGeneration(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like convexHullGeneration(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like convexHullCodegen(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like convexHullCodegen(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
        it should behave like convexHullGeneration(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like convexHullGeneration(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
    }

    "ST_ transformations" should "execute without errors for any index system and any geometry API" in {
        it should behave like transformationsCodegen(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like transformationsCodegen(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like transformationsCodegen(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like transformationsCodegen(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
    }

    "ST_ buffer" should "execute without errors for any index system and any geometry API" in {
        it should behave like bufferCalculation(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like bufferCalculation(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like bufferCodegen(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like bufferCodegen(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like bufferCalculation(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like bufferCalculation(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
        it should behave like bufferCodegen(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like bufferCodegen(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
    }

}
