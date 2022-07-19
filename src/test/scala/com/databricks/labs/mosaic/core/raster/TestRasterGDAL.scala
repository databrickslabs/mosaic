package com.databricks.labs.mosaic.core.raster

import java.nio.file.{Files, Paths}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class TestRasterGDAL extends AnyFlatSpec {
    "MosaicRasterGDAL" should "read raster metadata from a GeoTIFF file." in {
        val resourcePath = "/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"
        val inFile = getClass.getResource(resourcePath)
        val byteArray = Files.readAllBytes(Paths.get(inFile.getPath))
        val testRaster = MosaicRasterGDAL.fromBytes(byteArray)

        println(s"x-pixels: ${testRaster.xSize}, y-pixels: ${testRaster.ySize}")
        println(s"num bands: ${testRaster.numBands}")
        println("spatial ref metadata:")
        println(s"proj: ${testRaster.proj4String}, srid: ${testRaster.SRID}")
        println(s"geo transform coefficients: (0, 0) => (${testRaster.geoTransform(0, 0).mkString(",")})")
        println(s"extent: (${testRaster.extent.mkString(",")})")
        val testBand = testRaster.getBand(1)
        println(s"description: ${testBand.description}")
        println(s"data type: ${testBand.dataType}")
        println(s"min pixel value: ${testBand.minPixelValue}, max pixel value: ${testBand.maxPixelValue}")
        println(s"pixel value scale: ${testBand.pixelValueScale}, pixel value offset: ${testBand.pixelValueOffset}")
        val testValues = testBand.values(1000, 1000, 100, 50)
        val testValuesConverted = testValues.map(l => l.map(p => testBand.pixelValueToUnitValue(p)))
        println(testValuesConverted.map(l => l.mkString(",")).mkString("\n"))
    }
}
