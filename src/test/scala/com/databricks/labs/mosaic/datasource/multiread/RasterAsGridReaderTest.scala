package com.databricks.labs.mosaic.datasource.multiread

import com.databricks.labs.mosaic.JTS
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.io.RasterIO
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import com.databricks.labs.mosaic.utils.PathUtils.VSI_ZIP_TOKEN
import org.apache.spark.sql.test.SharedSparkSessionGDAL
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants.GA_ReadOnly
import org.scalatest.matchers.must.Matchers.{be, noException}
import org.scalatest.matchers.should.Matchers.{an, convertToAnyShouldWrapper}

import java.nio.file.{Files, Paths}
import java.util.{Vector => JVector}

class RasterAsGridReaderTest extends MosaicSpatialQueryTest with SharedSparkSessionGDAL {

    test("Read netcdf with Raster As Grid Reader") {
        assume(System.getProperty("os.name") == "Linux")
        val netcdf = "/binary/netcdf-coral/"
        val filePath = getClass.getResource(netcdf).getPath

        val sc = this.spark
        import sc.implicits._
        sc.sparkContext.setLogLevel("ERROR")

        // init
        val mc = MosaicContext.build(H3IndexSystem, JTS)
        mc.register(sc)
        import mc.functions._

        info(s"checkpoint dir? ${GDAL.getCheckpointDir}")
        //CleanUpManager.setCleanThreadDelayMinutes(300)

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("subdatasetName", "bleaching_alert_area")
            .option("nPartitions", "10")
            .option("extensions", "nc")
            .option("resolution", "0")
            .option("kRingInterpolate", "1")
            .load(s"$filePath/ct5km_baa-max-7d_v3.1_20220101.nc")
            .select("measure")
            .take(1)

    }

    test("Read grib with Raster As Grid Reader") {
        assume(System.getProperty("os.name") == "Linux")
        val grib = "/binary/grib-cams/"
        val filePath = getClass.getResource(grib).getPath

        val sc = this.spark
        import sc.implicits._
        sc.sparkContext.setLogLevel("ERROR")

        // init
        val mc = MosaicContext.build(H3IndexSystem, JTS)
        mc.register(sc)
        import mc.functions._

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
        val tif = "/modis/"
        val filePath = getClass.getResource(tif).getPath

        val sc = this.spark
        import sc.implicits._
        sc.sparkContext.setLogLevel("ERROR")

        // init
        val mc = MosaicContext.build(H3IndexSystem, JTS)
        mc.register(sc)
        import mc.functions._

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
        val zarr = "/binary/zarr-example/"
        val filePath = getClass.getResource(zarr).getPath

        val sc = this.spark
        import sc.implicits._
        sc.sparkContext.setLogLevel("ERROR")

        // init
        val mc = MosaicContext.build(H3IndexSystem, JTS)
        mc.register(sc)
        import mc.functions._

        info("- testing [[Dataset]] for Zarr subdataset -")
//        val rawPath = s"${VSI_ZIP_TOKEN}ZARR:${filePath}zarr_test_data.zip:/group_with_attrs/F_order_array" // <- NO
//        val rawPath = s"${VSI_ZIP_TOKEN}${filePath}zarr_test_data.zip:/group_with_attrs/F_order_array"      // <- NO
//        val rawPath = s"""${VSI_ZIP_TOKEN}${filePath}zarr_test_data.zip"""                                  // <- YES (JUST ZIP)
//        val rawPath = s"""${VSI_ZIP_TOKEN}${filePath}zarr_test_data.zip:/group_with_attrs/F_order_array"""  // <- NO
        val rawPath = s"""${VSI_ZIP_TOKEN}${filePath}zarr_test_data.zip/group_with_attrs/F_order_array"""     // <- YES

        info(s"rawPath -> ${rawPath}")

        val drivers = new JVector[String]() // java.util.Vector
        drivers.add("Zarr")
        val ds = gdal.OpenEx(rawPath, GA_ReadOnly, drivers)
        ds != null should be(true)
        info(s"ds description -> ${ds.GetDescription()}")
        info(s"ds rasters -> ${ds.GetRasterCount()}")
        info(s"ds files -> ${ds.GetFileList()}")
        info(s"ds tile-1 -> ${ds.GetRasterBand(1).GetDescription()}")

        info("- testing [[RasterIO.rawPathAsDatasetOpt]] for Zarr subdataset -")

        val ds1 = RasterIO.rawPathAsDatasetOpt(rawPath, Some("Zarr"), getExprConfigOpt)
        ds1.isDefined should be(true)
        info(s"ds1 description -> ${ds1.get.GetDescription()}")
        info(s"ds1 rasters -> ${ds1.get.GetRasterCount()}")
        info(s"ds1 files -> ${ds1.get.GetFileList()}")
        info(s"ds1 tile-1 -> ${ds1.get.GetRasterBand(1).GetDescription()}")

        info("- testing [[MosaicContext.read]] for Zarr subdataset -")

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("driverName", "Zarr") // <- needed?
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
            .option("driverName", "Zarr") // <- needed?
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
            .option("driverName", "Zarr") // <- needed?
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
            .option("driverName", "Zarr") // <- needed?
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
            .option("driverName", "Zarr") // <- needed?
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
            .option("driverName", "Zarr") // <- needed?
            .option("nPartitions", "10")
            .option("subdatasetName", "/group_with_attrs/F_order_array")
            .option("kRingInterpolate", "3")
            .load(filePath)
            .select("measure") // <- added
            .take(1)  // <- added
        info("... after subdataset + kring interpolate")

    }

}
