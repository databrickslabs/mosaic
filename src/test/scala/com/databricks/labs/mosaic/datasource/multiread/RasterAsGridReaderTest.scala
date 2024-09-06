package com.databricks.labs.mosaic.datasource.multiread

import com.databricks.labs.mosaic.JTS
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.DataFrame
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
            .option("combiner", "count_+") // <- invalid combiner (should fail early)
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

        var df: DataFrame = null
        var dfCnt = -1L
        for (stepTessellate <- Seq(false, true)) {
            df = MosaicContext.read
                .format("raster_to_grid")
                .option("nPartitions", "10")
                .option("extensions", "tif")
                .option("resolution", "2")
                .option("kRingInterpolate", "3")
                .option("verboseLevel", "1") // <- interim progress (0,1,2)?
                .option("limitTessellate", "10") // <- keeping rows down for testing
                .option("stepTessellate", "true") // <- allowed for tifs
                .load(s"${filePath}MCD43A4.A2018185.h10v07.006.2018194033728_B04.TIF")
                .select("measure")

            dfCnt = df.count()
            info(s"tif testing count - $dfCnt  (stepTessellate? $stepTessellate) ...")
            if (stepTessellate) dfCnt == 94 shouldBe (true)        // <- step tessellate (with `limit`)
            else dfCnt == 94 shouldBe (true)                       // <- tif or orig = same
        }
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
            .option("verboseLevel", "1")     // <- interim progress (0,1,2)?
            .option("limitTessellate", "10") // <- keeping rows down for testing
            .option("combiner", randomCombiner)
            .load(s"${filePath}MCD43A4.A2018185.h10v07.006.2018194033728_B04.TIF")
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

        var df: DataFrame = null
        var dfCnt = -1L
        for (toTif <- Seq(false, true)) {
            for (stepTessellate <- Seq(false, true)) {
                val df = MosaicContext.read
                    .format("raster_to_grid")
                    .option("resolution", "1")          // <- was 0, 1 for stepTessellate now
                    .option("nPartitions", "10")
                    .option("extensions", "grb")
                    .option("combiner", "min")
                    .option("kRingInterpolate", "3")
                    .option("verboseLevel", "1")        // <- interim progress (0,1,2)?
                    .option("limitTessellate", "10")    // <- keeping rows down for testing
                    .load(filePath)
                    .select("measure")

                dfCnt = df.count()
                info(s"grib testing count - $dfCnt  (toTif? $toTif, stepTessellate? $stepTessellate) ...")
                if (stepTessellate) dfCnt == 868 shouldBe (true)        // <- step tessellate (with `limit`)
                else dfCnt == 868 shouldBe (true)                       // <- tif or orig = same
            }
        }
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

        var df: DataFrame = null
        var dfCnt = -1L
        for (toTif <- Seq(false, true)) {
            for (stepTessellate <- Seq(false, true)) {
                df = MosaicContext.read
                    .format("raster_to_grid")
                    .option("subdatasetName", "bleaching_alert_area")
                    .option("nPartitions", "10")
                    .option("resolution", "1")
                    .option("kRingInterpolate", "1")
                    .option("verboseLevel", "1")     // <- interim progress (0,1,2)?
                    .option("limitTessellate", "10") // <- keeping rows down for testing
                    .option("sizeInMB", "-1")
                    .option("toTif", toTif.toString)
                    .option("stepTessellate", stepTessellate.toString)
                    .load(s"$filePath/ct5km_baa-max-7d_v3.1_20220101.nc")

                dfCnt = df.count()
                info(s"netcdf testing count - $dfCnt  (toTif? $toTif, stepTessellate? $stepTessellate) ...")
                if (stepTessellate) dfCnt == 32 shouldBe (true)        // <- step tessellate (with `limit`)
                else dfCnt == 68 shouldBe (true)                       // <- tif or orig = same
            }
        }
    }

}
