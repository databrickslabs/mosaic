package com.databricks.labs.mosaic.datasource.multiread

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.JTS
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.{SharedSparkSession, SharedSparkSessionGDAL}
import org.scalatest.matchers.must.Matchers.{be, noException}

class RasterAsGridReaderTest extends MosaicSpatialQueryTest with SharedSparkSessionGDAL {

    test("Read grib with Raster As Grid Reader") {
        assume(System.getProperty("os.name") == "Linux")
        MosaicContext.build(H3IndexSystem, JTS)

        val grib = "/binary/grib-cams/"
        val filePath = getClass.getResource(grib).getPath

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("fileExtension", "grib")
            .option("tileSize", "10")
            .option("kRingInterpolation", "3")
            .load(filePath)
            .select("measure")
            .take(1)

    }

    test("Read tif with Raster As Grid Reader") {
        assume(System.getProperty("os.name") == "Linux")
        MosaicContext.build(H3IndexSystem, JTS)

        val tif = "/modis/"
        val filePath = getClass.getResource(tif).getPath

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("tileSize", "10")
            .option("kRingInterpolation", "3")
            .load(filePath)
            .select("measure")
            .take(1)

    }

    test("Read zarr with Raster As Grid Reader") {
        assume(System.getProperty("os.name") == "Linux")
        MosaicContext.build(H3IndexSystem, JTS)

        val zarr = "/binary/zarr-example/"
        val filePath = getClass.getResource(zarr).getPath

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("vsizip", "true")
            .option("tileSize", "10")
            .option("kRingInterpolation", "3")
            .load(filePath)
            .select("measure")
            .take(1)

    }

}
