package com.databricks.labs.mosaic.datasource.multiread

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.JTS
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.matchers.must.Matchers.{be, noException}

class RasterAsGridReaderTest extends QueryTest with SharedSparkSession {

    test("Read netcdf with GDALFileFormat") {
        assume(System.getProperty("os.name") == "Linux")

        val netcdf = "/binary/netcdf-coral/"
        val filePath = getClass.getResource(netcdf).getPath

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("readSubdataset", "true")
            .option("subdatasetNumber", "0")
            .load(filePath)
            .take(1)

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("readSubdataset", "true")
            .option("subdatasetNumber", "0")
            .option("retile", "true")
            .option("tileSize", "256")
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
            .select("geometry")
            .take(1)

    }

    test("Read tif with GDALFileFormat") {
        assume(System.getProperty("os.name") == "Linux")

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
            .select("geometry")
            .take(1)

    }

    test("Read zarr with GDALFileFormat") {
        assume(System.getProperty("os.name") == "Linux")

        val zarr = "/binary/zarr-example/"
        val filePath = getClass.getResource(zarr).getPath

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
            .select("geometry")
            .take(1)

    }

}
