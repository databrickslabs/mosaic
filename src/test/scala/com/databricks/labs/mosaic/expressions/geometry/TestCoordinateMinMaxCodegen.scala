package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.raster.api.RasterAPI.GDAL
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkCodeGenSuite
import org.scalatest.flatspec.AnyFlatSpec

class TestCoordinateMinMaxCodegen extends AnyFlatSpec with CoordinateMinMaxBehaviors with SparkCodeGenSuite {

    "ST_xmin" should "return minimum x coordinate for any index system and any geometry API" in {
        it should behave like xMinCodegen(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like xMinCodegen(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like xMin(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like xMin(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like xMinCodegen(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like xMinCodegen(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
        it should behave like xMin(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like xMin(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
    }

    "ST_xmax" should "return maximum x coordinate for any index system and any geometry API" in {
        it should behave like xMaxCodegen(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like xMaxCodegen(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like xMax(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like xMax(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like xMaxCodegen(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like xMaxCodegen(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
        it should behave like xMax(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like xMax(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
    }

    "ST_ymin" should "return minimum y coordinate for any index system and any geometry API" in {
        it should behave like yMinCodegen(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like yMinCodegen(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like yMin(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like yMin(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like yMinCodegen(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like yMinCodegen(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
        it should behave like yMin(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like yMin(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
    }

    "ST_ymax" should "return maximum y coordinate for any index system and any geometry API" in {
        it should behave like yMaxCodegen(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like yMaxCodegen(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like yMax(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like yMax(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like yMaxCodegen(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like yMaxCodegen(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
        it should behave like yMax(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like yMax(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
    }

}
