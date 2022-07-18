package com.databricks.labs.mosaic.expressions.constructors

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.core.raster.MosaicRasterGDAL
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkSuite
import org.gdal.gdal.Dataset
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.file.{Files, Paths}

class TestConstructors extends AnyFlatSpec with ConstructorsBehaviors with SparkSuite {

    "ST_Point" should "construct a point geometry for any index system and any geometry API" in {
        it should behave like createST_Point(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like createST_Point(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ST_MakeLine" should "construct a line geometry from an array of points for any index system and any geometry API" in {
        it should behave like createST_MakeLineSimple(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like createST_MakeLineSimple(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ST_MakeLine" should "construct a line geometry from a set of geometries for any index system and any geometry API" in {
        it should behave like createST_MakeLineComplex(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like createST_MakeLineComplex(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ST_MakeLine" should "return null if any input is null" in {
        it should behave like createST_MakeLineAnyNull(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like createST_MakeLineAnyNull(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ST_MakePolygon" should "construct a polygon geometry without holes for any index system and any geometry API" in {
        it should behave like createST_MakePolygonNoHoles(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like createST_MakePolygonNoHoles(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ST_MakePolygon" should "construct a polygon geometry with holes for any index system and any geometry API" in {
        it should behave like createST_MakePolygonWithHoles(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like createST_MakePolygonWithHoles(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ReadFromGDAL" should "read a geotiff" in {
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

    "ReadFromGDAL" should "read a grib file" in {
        val resourcePath = "/binary/grib-cams/adaptor.mars.internal-1650626995.380916-11651-14-ca8e7236-16ca-4e11-919d-bdbd5a51da35.grib"
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
        val testValues = testBand.values

//        val testValuesConverted = testValues.map(l => l.map(p => testBand.pixelValueToUnitValue(p)))
        println(testValues.map(l => l.mkString(",")).mkString("\n"))

    }

    "ReadFromGDAL" should "read a netcdf file" in {
        val resourcePath = "/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220101.nc"
        val inFile = getClass.getResource(resourcePath)
        val byteArray = Files.readAllBytes(Paths.get(inFile.getPath))
        val testRaster = MosaicRasterGDAL.fromBytes(byteArray, 0)

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
        val testValues = testBand.values(1000, 5000, 10, 10)
        println(testValues.map(l => l.mkString(",")).mkString("\n"))
                val testValuesConverted = testValues.map(l => l.map(p => testBand.pixelValueToUnitValue(p)))
        println(testValuesConverted.map(l => l.mkString(",")).mkString("\n"))

    }

}
