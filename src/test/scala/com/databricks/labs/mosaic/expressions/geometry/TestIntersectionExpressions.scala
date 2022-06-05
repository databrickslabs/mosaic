package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkSuite
import org.scalatest.flatspec.AnyFlatSpec

class TestIntersectionExpressions extends AnyFlatSpec with IntersectionExpressionsBehaviors with SparkSuite {

    "ST_IntersectsAggregate" should "compute the intersects flag via aggregate expression" in {
        it should behave like intersects(MosaicContext.build(H3IndexSystem, ESRI), spark, 11)
        it should behave like intersects(MosaicContext.build(H3IndexSystem, JTS), spark, 11)
        it should behave like intersects(MosaicContext.build(BNGIndexSystem, ESRI), spark, 5)
        it should behave like intersects(MosaicContext.build(BNGIndexSystem, JTS), spark, 5)
    }

    "ST_IntersectionAggregate" should "compute the intersection via aggregate expression" in {
        it should behave like intersection(MosaicContext.build(H3IndexSystem, ESRI), spark, 9)
        it should behave like intersection(MosaicContext.build(H3IndexSystem, JTS), spark, 9)
        it should behave like intersection(MosaicContext.build(BNGIndexSystem, ESRI), spark, 5)
        it should behave like intersection(MosaicContext.build(BNGIndexSystem, JTS), spark, 5)
    }

}
