package com.databricks.labs.mosaic.datasource.multiread

import com.databricks.labs.mosaic.{JTS, MOSAIC_RASTER_READ_STRATEGY}
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.test.SharedSparkSessionGDAL
import org.scalatest.matchers.must.Matchers.{be, noException}
import org.scalatest.matchers.should.Matchers.an

import java.nio.file.{Files, Paths}

class RasterAsGridReaderTest extends MosaicSpatialQueryTest with SharedSparkSessionGDAL {

    test("Read netcdf with Raster As Grid Reader") {
        val netcdf = "/binary/netcdf-coral/"
        val filePath = getClass.getResource(netcdf).getPath

        val sc = this.spark
        import sc.implicits._

        assume(System.getProperty("os.name") == "Linux")
        val mc = MosaicContext.build(H3IndexSystem, JTS)
        mc.register(sc)
        import mc.functions._

//        val subs = spark.read.format("gdal")
//            .load(filePath)
//            .select("subdatasets")
//            .first.get(0)
//        info(s"subs -> $subs")
        //"bleaching_alert_area"

//        val subTile = spark.read
//            .format("gdal")
//            .option("extensions", "nc")
//            .option(MOSAIC_RASTER_READ_STRATEGY, "as_path")
//            .option("vsizip", "false")
//            .load(filePath)
//                .repartition(10)
//            .withColumn("tile", rst_getsubdataset($"tile", lit("bleaching_alert_area")))
//            .select("tile")
//            .first.get(0)
//        info(s"subTile -> $subTile")

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("subdatasetName", "bleaching_alert_area")
            .option("nPartitions", "10")
            .option("extensions", "nc")
            .option("resolution", "5")
            .option("kRingInterpolate", "3")
            .load(filePath)
            .select("measure")
            .queryExecution
            .executedPlan

    }

    test("Read grib with Raster As Grid Reader") {
        assume(System.getProperty("os.name") == "Linux")
        MosaicContext.build(H3IndexSystem, JTS)

        val grib = "/binary/grib-cams/"
        val filePath = getClass.getResource(grib).getPath

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("nPartitions", "10")
            .option("extensions", "grb")
            .option("combiner", "min")
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
            .option("nPartitions", "10")
            .option("extensions", "tif")
            .option("combiner", "max")
            .option("resolution", "4")
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
            .option("nPartitions", "10")
            .option("subdatasetName", "/group_with_attrs/F_order_array")
            .option("combiner", "median")
            .option("vsizip", "true")
            .load(filePath)
            .select("measure")
            .take(1)
        info("... after median combiner")

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("nPartitions", "10")
            .option("subdatasetName", "/group_with_attrs/F_order_array")
            .option("combiner", "count")
            .option("vsizip", "true")
            .load(filePath)
            .select("measure")
            .take(1)
        info("... after count combiner")

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("nPartitions", "10")
            .option("subdatasetName", "/group_with_attrs/F_order_array")
            .option("combiner", "average")
            .option("vsizip", "true")
            .load(filePath)
            .select("measure")
            .take(1)
        info("... after average combiner")

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("nPartitions", "10")
            .option("subdatasetName", "/group_with_attrs/F_order_array")
            .option("combiner", "avg")
            .option("vsizip", "true")
            .load(filePath)
            .select("measure")
            .take(1)
        info("... after avg combiner")

        val paths = Files.list(Paths.get(filePath)).toArray.map(_.toString)

        an[Error] should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("nPartitions", "10")
            .option("combiner", "count_+")
            .option("vsizip", "true")
            .load(paths: _*)
            .select("measure")
            .take(1)
        info("... after count_+ combiner (exception)")

        an[Error] should be thrownBy MosaicContext.read
            .format("invalid")
            .load(paths: _*)
        info("... after invalid paths format (exception)")

        an[Error] should be thrownBy MosaicContext.read
            .format("invalid")
            .load(filePath)
        info("... after invalid path format (exception)")

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("nPartitions", "10")
            .option("subdatasetName", "/group_with_attrs/F_order_array")
            .option("kRingInterpolate", "3")
            .load(filePath)
        info("... after subdataset + kring interpolate")

    }

}
