package com.databricks.labs.mosaic.datasource

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.matchers.must.Matchers.{be, noException}

class GDALBinaryFileFormatTest extends QueryTest with SharedSparkSession {

    test("Read netcdf with GDALFileFormat") {
        assume(System.getProperty("os.name") == "Linux")
        spark.sparkContext.setLogLevel("ERROR")

        val netcdf = "/binary/netcdf-coral/"
        val filePath = getClass.getResource(netcdf).getPath

        noException should be thrownBy spark.read
            .format("gdal_binary")
            .load(filePath)
            .show(1)


        spark.read
            .format("gdal_binary")
            .load(filePath)
            .show(20)

        spark.read.format("gdal_binary")
            .load(filePath)
            .select("path")
            .show()

    }

}
