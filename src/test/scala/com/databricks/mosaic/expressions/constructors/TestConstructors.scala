package com.databricks.mosaic.expressions.constructors

import org.scalatest.flatspec.AnyFlatSpec

import com.databricks.mosaic.core.geometry.api.GeometryAPI.{JTS, OGC}
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.test.SparkSuite

class TestConstructors extends AnyFlatSpec with ConstructorsBehaviors with SparkSuite {

    "ST_Point" should "construct a point geometry for any index system and any geometry API" in {
        it should behave like createST_Point(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like createST_Point(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ST_MakeLine" should "construct a line geometry from an array of points for any index system and any geometry API" in {
        it should behave like createST_MakeLineSimple(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like createST_MakeLineSimple(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ST_MakeLine" should "construct a line geometry from a set of geometries for any index system and any geometry API" in {
        it should behave like createST_MakeLineComplex(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like createST_MakeLineComplex(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ST_MakePolygon" should "construct a polygon geometry without holes for any index system and any geometry API" in {
        it should behave like createST_MakePolygonNoHoles(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like createST_MakePolygonNoHoles(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ST_MakePolygon" should "construct a polygon geometry with holes for any index system and any geometry API" in {
        it should behave like createST_MakePolygonWithHoles(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like createST_MakePolygonWithHoles(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

}
