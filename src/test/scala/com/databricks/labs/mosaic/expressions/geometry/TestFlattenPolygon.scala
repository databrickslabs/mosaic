package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.raster.api.RasterAPI.GDAL
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkSuite
import org.scalatest.flatspec.AnyFlatSpec

class TestFlattenPolygon extends AnyFlatSpec with FlattenPolygonBehaviors with SparkSuite {

    "Flatten Polygons Expression" should "flatten WKB polygons for any index system and any geometry API" in {
        it should behave like flattenWKBPolygon(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like flattenWKBPolygon(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like flattenWKBPolygon(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like flattenWKBPolygon(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
    }

    "Flatten Polygons Expression" should "flatten WKT polygons for any index system and any geometry API" in {
        it should behave like flattenWKTPolygon(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like flattenWKTPolygon(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like flattenWKTPolygon(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like flattenWKTPolygon(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
    }

    "Flatten Polygons Expression" should "flatten COORDS polygons for any index system and any geometry API" in {
        it should behave like flattenCOORDSPolygon(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like flattenCOORDSPolygon(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like flattenCOORDSPolygon(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like flattenCOORDSPolygon(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
    }

    "Flatten Polygons Expression" should "flatten HEX polygons for any index system and any geometry API" in {
        it should behave like flattenCOORDSPolygon(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like flattenCOORDSPolygon(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like flattenCOORDSPolygon(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like flattenCOORDSPolygon(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
    }

}
