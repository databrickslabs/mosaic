package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkSuite
import org.scalatest.flatspec.AnyFlatSpec

class TestFlattenPolygon extends AnyFlatSpec with FlattenPolygonBehaviors with SparkSuite {

    "Flatten Polygons Expression" should "flatten WKB polygons for any index system and any geometry API" in {
        it should behave like flattenWKBPolygon(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like flattenWKBPolygon(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like flattenWKBPolygon(MosaicContext.build(BNGIndexSystem, ESRI), spark)
        it should behave like flattenWKBPolygon(MosaicContext.build(BNGIndexSystem, JTS), spark)
    }

    "Flatten Polygons Expression" should "flatten WKT polygons for any index system and any geometry API" in {
        it should behave like flattenWKTPolygon(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like flattenWKTPolygon(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like flattenWKTPolygon(MosaicContext.build(BNGIndexSystem, ESRI), spark)
        it should behave like flattenWKTPolygon(MosaicContext.build(BNGIndexSystem, JTS), spark)
    }

    "Flatten Polygons Expression" should "flatten COORDS polygons for any index system and any geometry API" in {
        it should behave like flattenCOORDSPolygon(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like flattenCOORDSPolygon(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like flattenCOORDSPolygon(MosaicContext.build(BNGIndexSystem, ESRI), spark)
        it should behave like flattenCOORDSPolygon(MosaicContext.build(BNGIndexSystem, JTS), spark)
    }

    "Flatten Polygons Expression" should "flatten HEX polygons for any index system and any geometry API" in {
        it should behave like flattenCOORDSPolygon(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like flattenCOORDSPolygon(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like flattenCOORDSPolygon(MosaicContext.build(BNGIndexSystem, ESRI), spark)
        it should behave like flattenCOORDSPolygon(MosaicContext.build(BNGIndexSystem, JTS), spark)
    }

    "Flatten Polygons Expression" should "fail data type check for unexpected data type." in {
        it should behave like failDataTypeCheck(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like failDataTypeCheck(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like failDataTypeCheck(MosaicContext.build(BNGIndexSystem, ESRI), spark)
        it should behave like failDataTypeCheck(MosaicContext.build(BNGIndexSystem, JTS), spark)
    }

    "Flatten Polygons Expression" should "have correct auxiliary methods implementation." in {
        it should behave like auxiliaryMethods(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like auxiliaryMethods(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like auxiliaryMethods(MosaicContext.build(BNGIndexSystem, ESRI), spark)
        it should behave like auxiliaryMethods(MosaicContext.build(BNGIndexSystem, JTS), spark)
    }

}
