package com.databricks.labs.mosaic.datasource.multiread

import com.databricks.labs.mosaic.JTS
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSessionGDAL
import org.scalatest.matchers.must.Matchers.{be, noException}
import org.scalatest.matchers.should.Matchers.{an, convertToAnyShouldWrapper}

import java.nio.file.{Files, Paths}
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
            .option("resolution", "1")  // <- remote build struggles with rest=2
            .option("kRingInterpolate", "3")
            .option("verboseLevel", "2") // <- interim progress (0,1,2)?
            .load(filePath)
            .select("measure")
        df.count() == 61 shouldBe(true) // 102 for res=2
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
            .option("verboseLevel", "2") // <- interim progress (0,1,2)?
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
            .option("verboseLevel", "2") // <- interim progress (0,1,2)?
            .load(filePath)
            .select("measure")
        df.count() == 588 shouldBe(true)
    }


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

        val df = MosaicContext.read
            .format("raster_to_grid")
            .option("subdatasetName", "bleaching_alert_area")
            .option("nPartitions", "10")
            .option("resolution", "0")
            .option("kRingInterpolate", "1")
            .option("verboseLevel", "2") // <- interim progress (0,1,2)?
            .option("sizeInMB", "-1")
            .load(s"$filePath/ct5km_baa-max-7d_v3.1_20220101.nc")
            //.select("measure")
        df.count() == 122 shouldBe(true)
    }

}
