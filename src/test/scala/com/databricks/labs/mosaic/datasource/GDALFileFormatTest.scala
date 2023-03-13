package com.databricks.labs.mosaic.datasource

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.matchers.must.Matchers.{be, noException}

class GDALFileFormatTest extends QueryTest with SharedSparkSession {

    test("Read netcdf with GDALFileFormat") {
        assume(System.getProperty("os.name") == "Linux")

        val netcdf = "/binary/netcdf-coral/"
        val filePath = getClass.getResource(netcdf).getPath

        noException should be thrownBy spark.read
            .format("gdal")
            .load(filePath)
            .take(1)

        noException should be thrownBy spark.read
            .format("gdal")
            .option("driverName", "NetCDF")
            .load(filePath)
            .take(1)

        noException should be thrownBy spark.read
            .format("gdal")
            .option("driverName", "NetCDF")
            .load(filePath)
            .select("proj4Str")
            .take(1)

    }

    test("Read grib with GDALFileFormat") {
        assume(System.getProperty("os.name") == "Linux")

        val grib = "/binary/grib-cams/"
        val filePath = getClass.getResource(grib).getPath

        noException should be thrownBy spark.read
            .format("gdal")
            .load(filePath)
            .take(1)

        noException should be thrownBy spark.read
            .format("gdal")
            .option("driverName", "NetCDF")
            .load(filePath)
            .take(1)

        noException should be thrownBy spark.read
            .format("gdal")
            .option("driverName", "NetCDF")
            .load(filePath)
            .select("proj4Str")
            .take(1)

    }

    test("Read tif with GDALFileFormat") {
        assume(System.getProperty("os.name") == "Linux")

        val tif = "/modis/"
        val filePath = getClass.getResource(tif).getPath

        noException should be thrownBy spark.read
            .format("gdal")
            .load(filePath)
            .take(1)

        noException should be thrownBy spark.read
            .format("gdal")
            .option("driverName", "TIF")
            .load(filePath)
            .take(1)

        noException should be thrownBy spark.read
            .format("gdal")
            .option("driverName", "TIF")
            .load(filePath)
            .select("proj4Str")
            .take(1)

    }

    test("Read zarr with GDALFileFormat") {
        assume(System.getProperty("os.name") == "Linux")

        val zarr = "/binary/zarr-example/"
        val filePath = getClass.getResource(zarr).getPath

        noException should be thrownBy spark.read
            .format("gdal")
            .option("vsizip", "true")
            .load(filePath)
            .take(1)

        noException should be thrownBy spark.read
            .format("gdal")
            .option("driverName", "Zarr")
            .option("vsizip", "true")
            .load(filePath)
            .take(1)

        noException should be thrownBy spark.read
            .format("gdal")
            .option("driverName", "Zarr")
            .option("vsizip", "true")
            .load(filePath)
            .select("proj4Str")
            .take(1)

    }

}
