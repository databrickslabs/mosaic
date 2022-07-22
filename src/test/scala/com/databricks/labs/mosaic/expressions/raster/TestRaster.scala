package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.core.raster.api.RasterAPI.GDAL
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkSuite
import org.scalatest.flatspec.AnyFlatSpec

class TestRaster extends AnyFlatSpec with RasterBehaviors with SparkSuite {

    "ST_MetaData" should "return dataset metadata for any raster API" in {
        assume(System.getProperty("os.name") == "Linux")
        it should behave like readST_MetaData(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
    }

    "ST_Subdatasets" should "return paths and descriptions for subdatasets within a raster dataset for any raster API" in {
        assume(System.getProperty("os.name") == "Linux")
        it should behave like readST_Subdatasets(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
    }

    "ST_BandMetaData" should "return raster band metadata for any raster API" in {
        assume(System.getProperty("os.name") == "Linux")
        it should behave like readST_BandMetaData(MosaicContext.build(H3IndexSystem, ESRI, GDAL), spark)
    }

}
