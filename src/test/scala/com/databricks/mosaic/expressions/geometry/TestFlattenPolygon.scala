package com.databricks.mosaic.expressions.geometry

import org.scalatest.flatspec.AnyFlatSpec

import com.databricks.mosaic.core.geometry.api.GeometryAPI.{JTS, OGC}
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.test.SparkSuite

class TestFlattenPolygon extends AnyFlatSpec with FlattenPolygonBehaviors with SparkSuite {

    "Flatten Polygons Expression" should "flatten WKB polygons for any index system and any geometry API" in {
        it should behave like flattenWKBPolygon(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like flattenWKBPolygon(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "Flatten Polygons Expression" should "flatten WKT polygons for any index system and any geometry API" in {
        it should behave like flattenWKTPolygon(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like flattenWKTPolygon(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "Flatten Polygons Expression" should "flatten COORDS polygons for any index system and any geometry API" in {
        it should behave like flattenCOORDSPolygon(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like flattenCOORDSPolygon(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "Flatten Polygons Expression" should "flatten HEX polygons for any index system and any geometry API" in {
        it should behave like flattenCOORDSPolygon(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like flattenCOORDSPolygon(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

}
