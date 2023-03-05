package com.databricks.labs.mosaic.datasource

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.matchers.must.Matchers.{be, noException}

class OGRFileFormatTest extends QueryTest with SharedSparkSession {

    test("Read open geoDB with OGRFileFormat") {
        assume(System.getProperty("os.name") == "Linux")

        val geodb = "/binary/geodb/"
        val filePath = getClass.getResource(geodb).getPath

        noException should be thrownBy spark.read
            .format("ogr")
            .load(filePath)
            .take(1)

        noException should be thrownBy spark.read
            .format("ogr")
            .option("driverName", "FileGDB")
            .option("asWKB", "true")
            .load(filePath)
            .take(1)

        noException should be thrownBy spark.read
            .format("ogr")
            .option("driverName", "FileGDB")
            .option("asWKB", "true")
            .load(filePath)
            .select("geometry")
            .take(1)

    }

    test("Read shapefile with OGRFileFormat") {
        assume(System.getProperty("os.name") == "Linux")

        val shapefile = "/binary/shapefile/"
        val filePath = getClass.getResource(shapefile).getPath

        noException should be thrownBy spark.read
            .format("ogr")
            .load(filePath)
            .take(1)

        noException should be thrownBy spark.read
            .format("ogr")
            .option("driverName", "ESRI Shapefile")
            .option("asWKB", "true")
            .load(filePath)
            .take(1)

        noException should be thrownBy spark.read
            .format("ogr")
            .option("driverName", "ESRI Shapefile")
            .option("asWKB", "true")
            .load(filePath)
            .select("geometry")
            .take(1)

    }


}
