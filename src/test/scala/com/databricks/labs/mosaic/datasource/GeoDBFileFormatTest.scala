package com.databricks.labs.mosaic.datasource

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.matchers.must.Matchers.{be, noException}

class GeoDBFileFormatTest extends QueryTest with SharedSparkSession {

    test("Read open geoDB with GeoDBFileFormat") {
        assume(System.getProperty("os.name") == "Linux")

        val shapefile = "/binary/geodb/"
        val filePath = getClass.getResource(shapefile).getPath

        noException should be thrownBy spark.read
            .format("geo_db")
            .option("vsizip", "true")
            .load(filePath)
            .take(1)

        noException should be thrownBy spark.read
            .format("geo_db")
            .option("vsizip", "true")
            .option("asWKB", "true")
            .load(filePath)
            .take(1)

        noException should be thrownBy spark.read
            .format("geo_db")
            .option("vsizip", "true")
            .option("asWKB", "true")
            .load(filePath)
            .select("SHAPE_srid")
            .take(1)

    }

}
