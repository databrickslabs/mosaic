package com.databricks.labs.mosaic.expressions.constructors

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkSuite
import com.databricks.labs.mosaic.utils.NativeUtils
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants.GA_ReadOnly
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
        val libs = Seq(
            "/GDAL.libs/libcrypto-de69073a.so.0.9.8e",
            "/GDAL.libs/libcurl-9bc4ffbf.so.4.7.0",
            "/GDAL.libs/libexpat-c5a39682.so.1.6.11",
            "/GDAL.libs/libgeos--no-undefined-82dbfb1f.so",
            "/GDAL.libs/libgeos_c-d134951e.so.1.14.3",
            "/GDAL.libs/libhdf5-13db72d8.so.200.0.0",
            "/GDAL.libs/libhdf5_hl-76c8603c.so.200.0.0",
            "/GDAL.libs/libjasper-21c09ccf.so.1.0.0",
            "/GDAL.libs/libjson-c-ca0558d5.so.2.0.1",
            "/GDAL.libs/libnetcdf-f68a7300.so.13.1.1",
            "/GDAL.libs/libopenjp2-f1d08cc2.so.2.3.1",
            "/GDAL.libs/libsqlite3-13a07f98.so.0.8.6",
            "/GDAL.libs/libtiff-d64010d8.so.5.5.0",
            "/GDAL.libs/libwebp-25902a0b.so.7.1.0",
            "/GDAL.libs/libproj-268837f3.so.22.2.1",
            "/GDAL.libs/libgdal-39073f84.so.30.0.1",
            "/GDAL.libs/libgdalalljni.so",
        )
        libs.foreach(NativeUtils.loadLibraryFromJar)
        gdal.AllRegister()
        val resourcePath = "/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"
        val id = java.util.UUID.randomUUID
        val virtualPath = s"/vsimem/${id.toString}.TIF"
        val inFile = getClass.getResource(resourcePath)
        val byteArray = Files.readAllBytes(Paths.get(inFile.getPath))
        gdal.FileFromMemBuffer(virtualPath, byteArray)
        val dataset = gdal.Open(virtualPath, GA_ReadOnly)
        val band = dataset.GetRasterBand(1)
        println(s"x-pixels: ${band.getXSize}, y-pixels: ${band.getYSize}, spatial ref: ${dataset.GetSpatialRef.ExportToProj4}")
    }

}
