package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.raster.api.RasterAPI.GDAL
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkCodeGenSuite
import org.scalatest.flatspec.AnyFlatSpec

class TestIntersectionExpressionsCodegen extends AnyFlatSpec with IntersectionExpressionsBehaviors with SparkCodeGenSuite {

    "ST_IntersectsAggregate" should "compute the intersects flag via aggregate expression" in {
        it should behave like intersectsCodegen(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like intersectsCodegen(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like intersectsCodegen(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like intersectsCodegen(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
        it should behave like intersects(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark, 11)
        it should behave like intersects(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark, 11)
        it should behave like intersects(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark, 5)
        it should behave like intersects(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark, 5)
    }

    "ST_IntersectionAggregate" should "compute the intersection via aggregate expression" in {
        it should behave like intersectionCodegen(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like intersectionCodegen(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like intersectionCodegen(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like intersectionCodegen(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
        it should behave like intersection(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark, 9)
        it should behave like intersection(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark, 9)
        it should behave like intersection(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark, 4)
        it should behave like intersection(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark, 4)
    }

}
