package com.databricks.labs.mosaic.datasource

import com.databricks.labs.mosaic.MOSAIC_RASTER_READ_STRATEGY
import com.databricks.labs.mosaic.datasource.gdal.GDALFileFormat
import com.databricks.labs.mosaic.utils.ReaderUtils
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSessionGDAL
import org.scalatest.matchers.must.Matchers.{be, noException}
import org.scalatest.matchers.should.Matchers.an

class GDALFileFormatTest extends QueryTest with SharedSparkSessionGDAL {

    test("Read netcdf with GDALFileFormat") {
        assume(System.getProperty("os.name") == "Linux")

        val netcdf = "/binary/netcdf-ECMWF/"
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
            .select("metadata")
            .take(1)

    }

    test("Read tif with GDALFileFormat") {
        assume(System.getProperty("os.name") == "Linux")

        val tif = "/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"
        val filePath = getClass.getResource(tif).getPath

        noException should be thrownBy spark.read
            .format("gdal")
            .load(filePath)
            .repartition()
            .take(1)

        noException should be thrownBy spark.read
            .format("gdal")
            .option("driverName", "TIF")
            .load(filePath)
            .repartition()
            .take(1)

        spark.read
            .format("gdal")
            .option("driverName", "TIF")
            .load(filePath)
            .select("metadata")
            .repartition()
            .take(1)

        spark.read
            .format("gdal")
            .option(MOSAIC_RASTER_READ_STRATEGY, "retile_on_read")
            .load(filePath)
            .repartition()
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
            .select("metadata")
            .take(1)

    }

    test("GDALFileFormat utility tests") {
        val reader = new GDALFileFormat()
        an[Error] should be thrownBy reader.prepareWrite(spark, null, null, null)

        noException should be thrownBy ReaderUtils.createRow(Array(null))
        noException should be thrownBy ReaderUtils.createRow(Array(1, 2, 3))
        noException should be thrownBy ReaderUtils.createRow(Array(1.toByte))
        noException should be thrownBy ReaderUtils.createRow(Array("1"))
        noException should be thrownBy ReaderUtils.createRow(Array(Map("key" -> "value")))

    }

    test("Read grib with GDALFileFormat") {
        assume(System.getProperty("os.name") == "Linux")

        val grib = "/binary/grib-cams/"
        val filePath = getClass.getResource(grib).getPath

        spark.read
            .format("gdal")
            .option("extensions", "grb")
            .option("raster.read.strategy", "retile_on_read")
            .load(filePath)
            .take(1)

        noException should be thrownBy spark.read
            .format("gdal")
            .option("extensions", "grb")
            .option("raster.read.strategy", "retile_on_read")
            .load(filePath)
            .take(1)

        spark.read
            .format("gdal")
            .option("extensions", "grb")
            .option("raster.read.strategy", "retile_on_read")
            .load(filePath)
            .select("metadata")
            .take(1)

    }

}
