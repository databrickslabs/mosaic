package com.databricks.labs.mosaic.datasource.multiread

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.JTS
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.{SharedSparkSession, SharedSparkSessionGDAL}
import org.scalatest.matchers.must.Matchers.{be, noException}

class RasterAsGridReaderTest extends MosaicSpatialQueryTest with SharedSparkSessionGDAL {

    test("Read netcdf with GDALFileFormat") {
        assume(System.getProperty("os.name") == "Linux")
        MosaicContext.build(H3IndexSystem, JTS)

        val netcdf = "/binary/netcdf-coral/"
        val filePath = getClass.getResource(netcdf).getPath

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("resolution", "15")
            .option("readSubdataset", "true")
            .option("subdatasetNumber", "1")
            .load(filePath)
            .take(1)

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("resolution", "5")
            .option("readSubdataset", "true")
            .option("subdatasetNumber", "1")
            .option("retile", "true")
            .option("tileSize", "20")
            .load(filePath)
            .take(1)

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("readSubdataset", "true")
            .option("subdatasetNumber", "0")
            .option("kRingInterpolation", "3")
            .load(filePath)
            .select("geometry")
            .take(1)

    }

    test("Read grib with GDALFileFormat") {
        assume(System.getProperty("os.name") == "Linux")
        MosaicContext.build(H3IndexSystem, JTS)

        val grib = "/binary/grib-cams/"
        val filePath = getClass.getResource(grib).getPath

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .load(filePath)
            .take(1)

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("retile", "true")
            .option("tileSize", "256")
            .load(filePath)
            .take(1)

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("kRingInterpolation", "3")
            .load(filePath)
            .select("measure")
            .take(1)

    }

    test("Read tif with GDALFileFormat") {
        assume(System.getProperty("os.name") == "Linux")
        MosaicContext.build(H3IndexSystem, JTS)

        val tif = "/modis/"
        val filePath = getClass.getResource(tif).getPath

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .load(filePath)
            .take(1)

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("retile", "true")
            .option("tileSize", "256")
            .load(filePath)
            .take(1)

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("kRingInterpolation", "3")
            .load(filePath)
            .select("measure")
            .take(1)

    }

    test("Read zarr with GDALFileFormat") {
        assume(System.getProperty("os.name") == "Linux")
        MosaicContext.build(H3IndexSystem, JTS)

        val zarr = "/binary/zarr-example/"
        val filePath = getClass.getResource(zarr).getPath

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("vsizip", "true")
            .load(filePath)
            .take(1)

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("vsizip", "true")
            .option("retile", "true")
            .option("tileSize", "256")
            .load(filePath)
            .take(1)

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("vsizip", "true")
            .option("kRingInterpolation", "3")
            .load(filePath)
            .select("measure")
            .take(1)

    }

}
