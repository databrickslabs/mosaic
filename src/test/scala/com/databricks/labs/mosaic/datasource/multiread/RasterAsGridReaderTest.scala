package com.databricks.labs.mosaic.datasource.multiread

import com.databricks.labs.mosaic.core.Mosaic
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.{JTS, MOSAIC_CLEANUP_AGE_LIMIT_MINUTES, RASTER_DRIVER_KEY, RASTER_PATH_KEY}
import com.databricks.labs.mosaic.core.index.{H3IndexSystem, IndexSystem, IndexSystemFactory}
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.{DatasetGDAL, RasterGDAL}
import com.databricks.labs.mosaic.core.raster.io.{CleanUpManager, RasterIO}
import com.databricks.labs.mosaic.core.raster.operator.retile.RasterTessellate
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import com.databricks.labs.mosaic.utils.PathUtils.VSI_ZIP_TOKEN
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.test.SharedSparkSessionGDAL
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants.GA_ReadOnly
import org.gdal.osr
import org.scalatest.matchers.must.Matchers.{be, noException}
import org.scalatest.matchers.should.Matchers.{an, convertToAnyShouldWrapper}

import java.nio.file.{Files, Paths}
import java.util.{Vector => JVector}
import scala.util.Try
import scala.util.Random

class RasterAsGridReaderTest extends MosaicSpatialQueryTest with SharedSparkSessionGDAL {

    test("Read with Raster As Grid Reader - Exceptions") {
        // <<< NOTE: KEEP THIS FIRST (SUCCESS = FILE CLEANUP) >>>

        assume(System.getProperty("os.name") == "Linux")
        val tif = "/modis/"
        val filePath = getClass.getResource(tif).getPath
        val paths = Files.list(Paths.get(filePath)).toArray.map(_.toString)

        val sc = this.spark
        import sc.implicits._
        sc.sparkContext.setLogLevel("ERROR")

        // init
        val mc = MosaicContext.build(H3IndexSystem, JTS)
        mc.register(sc)
        import mc.functions._

        an[Error] should be thrownBy MosaicContext.read
            .format("invalid") // <- invalid format (path)
            .load(filePath)
        info("... after invalid path format (exception)")

        an[Error] should be thrownBy MosaicContext.read
            .format("invalid") // <- invalid format (paths)
            .load(paths: _*)
        info("... after invalid paths format (exception)")

        an[Error] should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("nPartitions", "10")
            .option("combiner", "count_+") // <- invalid combiner
            .load(paths: _*)
            .select("measure")
            .take(1)
        info("... after count_+ combiner (exception)")
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

        val df = MosaicContext.read
            .format("raster_to_grid")
            .option("nPartitions", "10")
            .option("extensions", "tif")
            .option("resolution", "2")
            .option("kRingInterpolate", "3")
            .option("verboseLevel", "2") // <- interim progress (0,1,2)?
            .load(filePath)
            .select("measure")
        df.count() == 102 shouldBe(true)
    }

    test("Read with Raster As Grid Reader - Various Combiners") {
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

        // all of these should work (very similar)
        // - they generate a lot of files which affects local testing
        // - so going with a random test
        val combinerSeq = Seq("average", "median", "count", "min", "max")
        val randomCombiner = combinerSeq(Random.nextInt(combinerSeq.size))

        noException should be thrownBy MosaicContext.read
            .format("raster_to_grid")
            .option("nPartitions", "10")
            .option("extensions", "tif")
            .option("resolution", "2")
            .option("kRingInterpolate", "3")
            .option("verboseLevel", "0") // <- interim progress (0,1,2)?
            .option("combiner", randomCombiner)
            .load(s"$filePath")
            .select("measure")
            .take(1)
        info(s"... after random combiner ('$randomCombiner')")

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

        val df = MosaicContext.read
            .format("raster_to_grid")
            .option("nPartitions", "10")
            .option("extensions", "grb")
            .option("combiner", "min")
            .option("kRingInterpolate", "3")
            .option("verboseLevel", "0") // <- interim progress (0,1,2)?
            .load(filePath)
            .select("measure")
        df.count() == 588 shouldBe(true)
    }


    test("Read netcdf with Raster As Grid Reader") {

        // TODO: FIX THIS FURTHER

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

        val df = MosaicContext.read
            .format("raster_to_grid")
            .option("stopAtTessellate", "true")  // <- TODO: should work without `stopAtTessellate` (fix)
            .option("subdatasetName", "bleaching_alert_area")
            .option("srid", "4326")         // <- TODO: should work without `srid` (fix)?
            .option("nPartitions", "10")
            .option("resolution", "0")
            .option("kRingInterpolate", "1")
            .option("skipProject", "true")  // <- TODO: should work without `skipProject` (fix)?
            .option("verboseLevel", "0") // <- interim progress (0,1,2)?
            .option("sizeInMB", "-1")
            .load(s"$filePath/ct5km_baa-max-7d_v3.1_20220101.nc")
            //.select("measure")
        df.count() == 122 shouldBe(true)
    }

//    test("Read zarr with Raster As Grid Reader") {
//
//        // TODO: FIX THIS FURTHER
//
//        assume(System.getProperty("os.name") == "Linux")
//        val zarr = "/binary/zarr-example/"
//        val filePath = getClass.getResource(zarr).getPath
//        info(s"filePath -> ${filePath}")
//        val sc = this.spark
//        import sc.implicits._
//        sc.sparkContext.setLogLevel("ERROR")
//
//        // init
//        val mc = MosaicContext.build(H3IndexSystem, JTS)
//        mc.register(sc)
//        import mc.functions._
//
//        info("- testing [[Dataset]] for Zarr subdataset -")
//        /*
//          val rawPath = s"${VSI_ZIP_TOKEN}ZARR:${filePath}zarr_test_data.zip:/group_with_attrs/F_order_array" // <- NO
//          val rawPath = s"${VSI_ZIP_TOKEN}${filePath}zarr_test_data.zip:/group_with_attrs/F_order_array"      // <- NO
//          val rawPath = s"""${VSI_ZIP_TOKEN}${filePath}zarr_test_data.zip"""                                  // <- YES (JUST ZIP)
//          val rawPath = s"""${VSI_ZIP_TOKEN}${filePath}zarr_test_data.zip:/group_with_attrs/F_order_array"""  // <- NO
//         */
//        //val rawPath = s"""${VSI_ZIP_TOKEN}${filePath}zarr_test_data.zip"""                                  // <- NO
//        val rawPath = s"""${VSI_ZIP_TOKEN}${filePath}zarr_test_data.zip/group_with_attrs/F_order_array"""       // <- YES
//        info(s"rawPath -> ${rawPath}")
//
//        val drivers = new JVector[String]() // java.util.Vector
//        drivers.add("Zarr")
//        val ds = gdal.OpenEx(rawPath, GA_ReadOnly, drivers)
//        ds != null should be(true)
//        info(s"ds description -> ${ds.GetDescription()}")
//        info(s"ds rasters -> ${ds.GetRasterCount()}")
//        info(s"ds files -> ${ds.GetFileList()}")
//        info(s"ds tile-1 -> ${ds.GetRasterBand(1).GetDescription()}")
//
//        info("- testing [[RasterIO.rawPathAsDatasetOpt]] for Zarr subdataset -")
//
//        val ds1 = RasterIO.rawPathAsDatasetOpt(rawPath, subNameOpt = None, driverNameOpt = Some("Zarr"), getExprConfigOpt)
//        ds1.isDefined should be(true)
//        info(s"ds1 description -> ${ds1.get.GetDescription()}")
//        info(s"ds1 num rasters -> ${ds1.get.GetRasterCount()}")     // <  1
//        Try(info(s"ds1 layer count -> ${ds1.get.GetLayerCount()}")) // <- 0
//        info(s"ds1 files -> ${ds1.get.GetFileList()}")              // <- 1
//        info(s"ds1 band-1 description -> ${ds1.get.GetRasterBand(1).GetDescription()}")
//        info(s"ds1 band-1 raster data type -> ${ds1.get.GetRasterBand(1).GetRasterDataType()}") // <- 5
//        // work with statistics
//        val ds1Stats = ds1.get.GetRasterBand(1).AsMDArray().GetStatistics()
//        info(s"ds1 band-1 valid count -> ${ds1Stats.getValid_count}") // <- 380
//        info(s"ds1 band-1 statistics -> min? ${ds1Stats.getMin}, max? ${ds1Stats.getMax}, mean? ${ds1Stats.getMean}, " +
//            s"std_dev? ${ds1Stats.getStd_dev}")
//        info(s"ds1 meta domains -> ${ds1.get.GetMetadataDomainList()}")
//
//        info("- testing manual tessellation steps for Zarr subdataset -")
//
//        val raster1 = RasterGDAL(
//            ds1.get,
//            getExprConfigOpt,
//            createInfo = Map(RASTER_PATH_KEY -> rawPath, RASTER_DRIVER_KEY -> "Zarr")
//        )
//
//        val geometryAPI: GeometryAPI = GeometryAPI.apply(getExprConfigOpt.orNull.getGeometryAPI)
//        // default destCRS is WGS84 (so good for this test)
//        val indexSystem: IndexSystem = IndexSystemFactory.getIndexSystem(getExprConfigOpt.orNull.getIndexSystem)
//        val indexSR = indexSystem.osrSpatialRef
//        //val bbox = raster1.bbox(geometryAPI, skipTransform = false) // <- if skipTransform = false get POLYGON EMPTY
//        val bbox = raster1.bbox(geometryAPI, destCRS = indexSR, skipTransform = true) // <- POLYGON ((0 0, 20 0, 20 20, 0 20, 0 0))
//        info(s"raster1 bbox (as WKT) -> ${bbox.toWKT}")
//
//        val cells = Mosaic.mosaicFill(bbox, 0, keepCoreGeom = false, indexSystem, geometryAPI)
//        info(s"raster1 cells length? ${cells.length}")
//
//        val tess = RasterTessellate.tessellate(
//            raster = raster1,
//            resolution = 0,
//            skipProject = true,
//            indexSystem,
//            geometryAPI,
//            getExprConfigOpt
//        )
//        info(s"tessellate length? ${tess.length}")
//        info(s"tessellate results -> ${tess}")
//
//        info("- testing [[MosaicContext.read]] for Zarr subdataset -")
//
//        //initial load ok
//        val dfZarr = spark.read.format("gdal")
//            .option("driverName", "Zarr")
//            .option("vsizip", "true")
//            .option("subdatasetName", "/group_with_attrs/F_order_array")
//            .load(filePath)
//            //.withColumn("tile", rst_getsubdataset($"tile", lit("/group_with_attrs/F_order_array")))
//            .withColumn("tile", rst_tessellate($"tile", lit(0), lit(true))) // <- skipProject = true
//        info(s"... 'gdal' zarr - count? ${dfZarr.count()}")
//        info(s"row -> ${dfZarr.first().toString()}")
//        dfZarr.show()
//
//        dfZarr.select("tile").show(truncate = false)
//
//        // subdataset seems ok
//        val dfZarrSub = dfZarr
//            .withColumn("tile", rst_getsubdataset($"tile", "/group_with_attrs/F_order_array"))
//        info(s"... 'gdal' zarr subdata - count? ${dfZarrSub.count()}")
//        //info(s"row -> ${dfZarrSub.first().toString()}")
//
//        dfZarrSub.select("tile").show(truncate = false)
//
//        // bounds are good
//        val dfZarrBounds = dfZarrSub
//            .withColumn("bounds", st_astext(rst_boundingbox($"tile")))
//        info(s"... 'gdal' zarr bounds - count? ${dfZarrBounds.count()}")
//        info(s"row -> ${dfZarrBounds.select("bounds").first().toString()}")
//
//        // tessellate throws exception
//        // - with / without 4326 SRID
//        val dfZarrTess = dfZarrSub
//            .withColumn("tile", rst_setsrid($"tile", lit(4326)))
//            .withColumn("tile", rst_tessellate($"tile", 0))
//        info(s"... 'gdal' zarr tessellate - count? ${dfZarrTess.count()}")
//        info(s"row -> ${dfZarrTess.first().toString()}")
//
//        val df = MosaicContext.read
//            .format("raster_to_grid")
//            .option("stopAtTessellate", "true")  // <- TODO: should work without `stopAtTessellate` (fix)
//            .option("driverName", "Zarr")  // <- needed
//            .option("skipProject", "true") // <- needed (0.4.3+)
//            .option("nPartitions", "10")
//            .option("subdatasetName", "/group_with_attrs/F_order_array") // <- needed
//            .option("vsizip", "true")
//            .option("combiner", "count")   // TODO - 'median' and 'average' throw exception; other come back with empty measures
//            .option("verboseLevel", "0")  // interim results (0,1,2)
//            .load(filePath)
//        df.count() == 5 shouldBe(true)
//        //df.show()
//    }

}
