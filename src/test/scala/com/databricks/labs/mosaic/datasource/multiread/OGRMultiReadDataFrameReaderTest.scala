package com.databricks.labs.mosaic.datasource.multiread

import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSessionGDAL
import org.scalatest.matchers.must.Matchers.{be, noException}

class OGRMultiReadDataFrameReaderTest extends QueryTest with SharedSparkSessionGDAL {

    test("Read open geoDB with OGRFileFormat") {
        assume(System.getProperty("os.name") == "Linux")

        val geodb = "/binary/geodb/"
        val filePath = getClass.getResource(geodb).getPath

        noException should be thrownBy MosaicContext.read
            .format("multi_read_ogr")
            .load(filePath)
            .take(1)

        noException should be thrownBy MosaicContext.read
            .format("multi_read_ogr")
            .option("driverName", "FileGDB")
            .option("asWKB", "true")
            .load(filePath)
            .take(1)

        noException should be thrownBy MosaicContext.read
            .format("multi_read_ogr")
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

        noException should be thrownBy MosaicContext.read
            .format("multi_read_ogr")
            .load(filePath)
            .take(1)

        noException should be thrownBy MosaicContext.read
            .format("multi_read_ogr")
            .option("driverName", "ESRI Shapefile")
            .option("asWKB", "true")
            .load(filePath)
            .take(1)

        noException should be thrownBy MosaicContext.read
            .format("multi_read_ogr")
            .option("driverName", "ESRI Shapefile")
            .option("asWKB", "true")
            .load(filePath)
            .select("geometry")
            .take(1)

    }

}
