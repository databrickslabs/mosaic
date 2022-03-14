package com.databricks.mosaic.expressions.geometry

import org.scalatest.flatspec.AnyFlatSpec

import com.databricks.mosaic.core.geometry.api.GeometryAPI.{JTS, OGC}
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.test.SparkSuite

class TestIntersectionExpressions extends AnyFlatSpec with IntersectionExpressionsBehaviors with SparkSuite {

    "ST_IntersectsAggregate" should "compute the intersects flag via aggregate expression" in {
        it should behave like intersects(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like intersects(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ST_IntersectionAggregate" should "compute the intersection via aggregate expression" in {
        it should behave like intersection(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like intersection(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

}
