package com.databricks.labs.mosaic

import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.QueryTest
import org.scalatest.funsuite.AnyFunSuite

class DatasourceDebug extends QueryTest with SharedSparkSession {

    test("Debug Datasource") {
        val raster = "/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220101.nc"
        val filePath = getClass.getResource(raster).getPath

        val df = spark.read.format("binaryFile")
            .option("pathGlobFilter", "*.nc")
            .option("recursiveFileLookup", "true")
            .load(filePath)

    }

}
