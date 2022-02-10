package com.databricks.mosaic.expressions.geometry

import org.scalatest.flatspec.AnyFlatSpec

import com.databricks.mosaic.core.geometry.api.GeometryAPI.{JTS, OGC}
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.test.SparkSuite

class TestValidity extends AnyFlatSpec with ValidityBehaviors with SparkSuite {

    "ST_IsValid" should "return true for valid geometries for any index system and any geometry API" in {
        it should behave like validGeometries(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like validGeometries(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ST_IsValid" should "return false for invalid geometries for any index system and any geometry API" in {
        it should behave like invalidGeometries(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like invalidGeometries(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

}
