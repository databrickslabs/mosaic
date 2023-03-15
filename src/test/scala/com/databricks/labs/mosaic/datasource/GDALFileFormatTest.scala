package com.databricks.labs.mosaic.datasource

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.matchers.must.Matchers.{be, noException, not}
import org.scalatest.matchers.should.Matchers.{an, convertToAnyShouldWrapper}

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

    test("GDALFileFormat utility tests") {
        val reader = new GDALFileFormat()
        an[Error] should be thrownBy reader.prepareWrite(spark, null, null, null)

        for (
          driver <- Seq(
            "GTiff",
            "HDF4",
            "HDF5",
            "JP2ECW",
            "JP2KAK",
            "JP2MrSID",
            "JP2OpenJPEG",
            "NetCDF",
            "PDF",
            "PNG",
            "VRT",
            "XPM",
            "COG",
            "GRIB",
            "Zarr"
          )
        ) {
            GDALFileFormat.getFileExtension(driver) should not be "UNSUPPORTED"
        }

        GDALFileFormat.getFileExtension("NotADriver") should be("UNSUPPORTED")

        noException should be thrownBy Utils.createRow(Array(null))
        noException should be thrownBy Utils.createRow(Array(1, 2, 3))
        noException should be thrownBy Utils.createRow(Array(1.toByte))
        noException should be thrownBy Utils.createRow(Array("1"))
        noException should be thrownBy Utils.createRow(Array(Map("key" -> "value")))

    }

}
