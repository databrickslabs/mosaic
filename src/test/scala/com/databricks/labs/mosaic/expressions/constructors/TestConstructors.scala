package com.databricks.labs.mosaic.expressions.constructors

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{JTS, ESRI}
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkSuite
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

    "CDMStructure" should "extract the variables and attributes of a set of netCDF files" in {
        it should behave like structureFromNetCDF(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like structureFromNetCDF(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    ignore should "extract the variables and attributes of a set of GRIB files" in {
        // https://github.com/Unidata/netcdf-java/issues/638
        it should behave like structureFromGRIB(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like structureFromGRIB(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    ignore should "extract the variables and attributes of a Zarr file" in {
        // debugging to see how to get the Zarr reader to recognise zip binary content
        it should behave like structureFromZarr(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like structureFromZarr(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "CDMContent" should "extract the content of a set of netCDF files" in {
        it should behave like contentFromNetCDF(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like contentFromNetCDF(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

}
