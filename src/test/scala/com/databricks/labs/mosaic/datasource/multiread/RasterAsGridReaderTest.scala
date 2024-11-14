package com.databricks.labs.mosaic.datasource.multiread

import com.databricks.labs.mosaic.JTS
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSessionGDAL
import org.scalatest.Tag
import org.scalatest.matchers.must.Matchers.{be, noException}
import org.scalatest.matchers.should.Matchers.{an, convertToAnyShouldWrapper}

import java.nio.file.{Files, Paths}

object ExcludeLocalTag extends Tag("ExcludeIfLocal")

class RasterAsGridReaderTest extends MosaicSpatialQueryTest with SharedSparkSessionGDAL {

    test("Read netcdf with Raster As Grid Reader") {
        assume(System.getProperty("os.name") == "Linux")
        assume(checkpointingEnabled)
        val mc = MosaicContext.build(H3IndexSystem, JTS)
        mc.register(spark)


        val netcdf = "/binary/netcdf-coral/"
        val filePath = this.getClass.getResource(netcdf).getPath

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("sizeInMB", "16")
            .option("resolution", "0")
            .option("readSubdataset", "true")
            .option("subdatasetName", "bleaching_alert_area")
            .option("retile", "true")
            .option("tileSize", "600")
            .option("kRingInterpolate", "3")
            .option("combiner", "avg")
            .load(filePath)
            .select("measure")
            .take(1)

    }

    test("Read ECMWF netcdf with Raster As Grid Reader") {
        assume(System.getProperty("os.name") == "Linux")
        assume(checkpointingEnabled)
        val mc = MosaicContext.build(H3IndexSystem, JTS)
        mc.register(spark)


        val netcdf = "/binary/netcdf-ECMWF/"
        val filePath = this.getClass.getResource(netcdf).getPath

        val result = MosaicContext.read
            .format("raster_to_grid")
            .option("sizeInMB", "16")
            .option("convertToFormat", "GTiff")
            .option("resolution", "0")
            .option("readSubdataset", "true")
            .option("subdatasetName", "t2m")
            .option("retile", "true")
            .option("tileSize", "600")
            .option("combiner", "avg")
            .load(filePath)
            .select("measure")
            .cache()

        result.count shouldBe 1098

        noException should be thrownBy result.take(1)

    }

    test("Read grib with Raster As Grid Reader", ExcludeLocalTag) {
        assume(System.getProperty("os.name") == "Linux")
        MosaicContext.build(H3IndexSystem, JTS)

        val grib = "/binary/grib-cams/"
        val filePath = getClass.getResource(grib).getPath

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("sizeInMB", "16")
            .option("resolution", "0")
            .option("extensions", "grib")
            .option("combiner", "min")
            .option("retile", "true")
            .option("tileSize", "100")
            .option("kRingInterpolate", "3")
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
            .option("sizeInMB", "16")
            .option("resolution", "0")
            .option("retile", "true")
            .option("tileSize", "100")
            .option("combiner", "max")
            .option("kRingInterpolate", "3")
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
            .option("readSubdataset", "true")
            .option("subdatasetName", "/group_with_attrs/F_order_array")
            .option("combiner", "median")
            .option("vsizip", "true")
            .option("tileSize", "10")
            .load(filePath)
            .select("measure")
            .take(1)

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("readSubdataset", "true")
            .option("subdatasetName", "/group_with_attrs/F_order_array")
            .option("combiner", "count")
            .option("vsizip", "true")
            .load(filePath)
            .select("measure")
            .take(1)

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("readSubdataset", "true")
            .option("subdatasetName", "/group_with_attrs/F_order_array")
            .option("combiner", "average")
            .option("vsizip", "true")
            .load(filePath)
            .select("measure")
            .take(1)

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("readSubdataset", "true")
            .option("subdatasetName", "/group_with_attrs/F_order_array")
            .option("combiner", "avg")
            .option("vsizip", "true")
            .load(filePath)
            .select("measure")
            .take(1)

        val paths = Files.list(Paths.get(filePath)).toArray.map(_.toString)

        an[Error] should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("combiner", "count_+")
            .option("vsizip", "true")
            .load(paths: _*)
            .select("measure")
            .take(1)

        an[Error] should be thrownBy MosaicContext.read
            .format("invalid")
            .load(paths: _*)

        an[Error] should be thrownBy MosaicContext.read
            .format("invalid")
            .load(filePath)

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("readSubdataset", "true")
            .option("subdatasetName", "/group_with_attrs/F_order_array")
            .option("kRingInterpolate", "3")
            .load(filePath)

    }

}
