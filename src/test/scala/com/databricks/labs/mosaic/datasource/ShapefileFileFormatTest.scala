package com.databricks.labs.mosaic.datasource

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.{SharedSparkSession, SharedSparkSessionGDAL}
import org.scalatest.matchers.must.Matchers.{be, noException}

class ShapefileFileFormatTest extends QueryTest with SharedSparkSessionGDAL {

    test("Read shapefile with ShapefileFileFormat") {
        assume(System.getProperty("os.name") == "Linux")

        val shapefile = "/binary/shapefile/"
        val filePath = getClass.getResource(shapefile).getPath

        noException should be thrownBy spark.read
            .format("shapefile")
            .load(filePath)
            .take(1)

        noException should be thrownBy spark.read
            .format("shapefile")
            .option("asWKB", "true")
            .load(filePath)
            .take(1)

        noException should be thrownBy spark.read
            .format("shapefile")
            .option("asWKB", "true")
            .load(filePath)
            .select("geom_0")
            .take(1)

    }

}
