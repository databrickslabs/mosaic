package com.databricks.labs.mosaic.codegen

import com.databricks.labs.mosaic.core.geometry.api.{ESRI, JTS}
import com.databricks.labs.mosaic.core.raster.api.RasterAPI.GDAL
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkCodeGenSuite
import org.scalatest.flatspec.AnyFlatSpec

class TestAsHexCodegen extends AnyFlatSpec with AsHexCodegenBehaviors with SparkCodeGenSuite {

    "AsHex Expression" should "do codegen for any index system and any geometry API" in {
        it should behave like codeGeneration(MosaicContext.build(H3IndexSystem, ESRI, GDAL))
        it should behave like codeGeneration(MosaicContext.build(H3IndexSystem, JTS, GDAL))
    }

}
