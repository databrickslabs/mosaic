package com.databricks.labs.mosaic.expressions.constructors

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkSuite
import com.databricks.labs.mosaic.utils.NativeUtils
//import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants.GA_ReadOnly
import org.scalatest.flatspec.AnyFlatSpec

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
        val nu = NativeUtils()
        nu.loadLibraryFromJar("/CRSBounds.csv")
//        val inFile = getClass.getResource("/modis/MCMCD43A4.A2018185.h10v07.006.2018194033728_B02.TIF")
//        val dataset = gdal.Open(inFile.getPath, GA_ReadOnly)
//        val band = dataset.GetRasterBand(1)
    }

}
