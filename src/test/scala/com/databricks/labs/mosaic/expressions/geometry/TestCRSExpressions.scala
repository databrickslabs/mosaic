package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkSuite
import org.scalatest.flatspec.AnyFlatSpec

class TestCRSExpressions extends AnyFlatSpec with CRSExpressionsBehaviours with SparkSuite {

    "ST_SRID" should "return the correct SRID for the input geometry" in {
        it should behave like extractSRID(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like extractSRID(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like extractSRID(MosaicContext.build(BNGIndexSystem, ESRI), spark)
        it should behave like extractSRID(MosaicContext.build(BNGIndexSystem, JTS), spark)
    }

    "ST_SetSRID" should "set the correct SRID for the input geometry" in {
        it should behave like assignSRID(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like assignSRID(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like assignSRID(MosaicContext.build(BNGIndexSystem, ESRI), spark)
        it should behave like assignSRID(MosaicContext.build(BNGIndexSystem, JTS), spark)
    }

    "ST_Transform" should "correctly reproject geometries into a new coordinate reference system" in {
        it should behave like reprojectGeometries(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like reprojectGeometries(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like reprojectGeometries(MosaicContext.build(BNGIndexSystem, ESRI), spark)
        it should behave like reprojectGeometries(MosaicContext.build(BNGIndexSystem, JTS), spark)
    }

}
