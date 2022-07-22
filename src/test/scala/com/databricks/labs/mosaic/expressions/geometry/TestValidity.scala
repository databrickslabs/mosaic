package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.raster.api.RasterAPI.GDAL
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkSuite
import org.scalatest.flatspec.AnyFlatSpec

class TestValidity extends AnyFlatSpec with ValidityBehaviors with SparkSuite {

    "ST_IsValid" should "return true for valid geometries for any index system and any geometry API" in {
        it should behave like validGeometries(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like validGeometries(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like validGeometries(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like validGeometries(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
    }

    "ST_IsValid" should "return false for invalid geometries for any index system and any geometry API" in {
        it should behave like invalidGeometries(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
        it should behave like invalidGeometries(MosaicContext.build(H3IndexSystem, JTS, GDAL), spark)
        it should behave like invalidGeometries(MosaicContext.build(BNGIndexSystem, ESRI, GDAL), spark)
        it should behave like invalidGeometries(MosaicContext.build(BNGIndexSystem, JTS, GDAL), spark)
    }

}
