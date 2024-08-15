package com.databricks.labs.mosaic.datasource

import com.databricks.labs.mosaic.{MOSAIC_RASTER_READ_STRATEGY, MOSAIC_RASTER_SUBDIVIDE_ON_READ}
import com.databricks.labs.mosaic.datasource.gdal.GDALFileFormat
import org.apache.spark.sql.test.SharedSparkSessionGDAL
import org.scalatest.matchers.must.Matchers.{be, noException}
import org.scalatest.matchers.should.Matchers.an

class GDALFileFormatTest extends SharedSparkSessionGDAL {

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
            .option("driverName", "netCDF")
            .load(filePath)
            .take(1)

        noException should be thrownBy spark.read
            .format("gdal")
            .option("driverName", "netCDF")
            .load(filePath)
            .select("metadata")
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
            .option("driverName", "GTiff") // TIF
            .load(filePath)
            .take(1)

        spark.read
            .format("gdal")
            .option("driverName", "GTiff") // TIF
            .load(filePath)
            .select("metadata")
            .take(1)

       spark.read
            .format("gdal")
            .option(MOSAIC_RASTER_READ_STRATEGY, MOSAIC_RASTER_SUBDIVIDE_ON_READ)
            .load(filePath)
            .collect()

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
            .select("metadata")
            .take(1)

    }

    test("GDALFileFormat utility tests") {
        val reader = new GDALFileFormat()
        an[Error] should be thrownBy reader.prepareWrite(spark, null, null, null)

        noException should be thrownBy Utils.createRow(Array(null))
        noException should be thrownBy Utils.createRow(Array(1, 2, 3))
        noException should be thrownBy Utils.createRow(Array(1.toByte))
        noException should be thrownBy Utils.createRow(Array("1"))
        noException should be thrownBy Utils.createRow(Array(Map("key" -> "value")))

    }

    test("Read grib with GDALFileFormat") {
        assume(System.getProperty("os.name") == "Linux")

        val grib = "/binary/grib-cams/"
        val filePath = getClass.getResource(grib).getPath

       spark.read
            .format("gdal")
            .option("extensions", "grb")
            .option(MOSAIC_RASTER_READ_STRATEGY, MOSAIC_RASTER_SUBDIVIDE_ON_READ)
            .load(filePath)
            .take(1)

        noException should be thrownBy spark.read
            .format("gdal")
            .option("extensions", "grb")
            .option(MOSAIC_RASTER_READ_STRATEGY, MOSAIC_RASTER_SUBDIVIDE_ON_READ)
            .load(filePath)
            .take(1)

        spark.read
            .format("gdal")
            .option("extensions", "grb")
            .option(MOSAIC_RASTER_READ_STRATEGY, MOSAIC_RASTER_SUBDIVIDE_ON_READ)
            .load(filePath)
            .select("metadata")
            .take(1)

    }

}
