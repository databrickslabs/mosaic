package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.{BAND_META_GET_KEY, RASTER_PATH_KEY}
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.io.RasterIO
import com.databricks.labs.mosaic.functions.{ExprConfig, MosaicContext}
import com.databricks.labs.mosaic.utils.PathUtils.VSI_ZIP_TOKEN
import com.databricks.labs.mosaic.utils.{FileUtils, PathUtils}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.functions.{size, _}
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants.GA_ReadOnly
import org.gdal.osr
import org.scalatest.matchers.should.Matchers._

import java.nio.file.{Files, Paths}
import java.util.{Vector => JVector}
import scala.util.Try

trait RST_TessellateBehaviors extends QueryTest {
    // noinspection MapGetGet
    def tessellateBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val sc = this.spark
        import sc.implicits._
        sc.sparkContext.setLogLevel("ERROR")

        // init
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(sc)
        import mc.functions._

        val exprConfigOpt = Some(ExprConfig(sc))

        var filePath: String = null
        var rawPath: String = null
        var ds: Dataset = null
        val srs4326 = new osr.SpatialReference()
        srs4326.ImportFromEPSG(4326)
        var drivers = new JVector[String]() // java.util.Vector

        // ::: [1] TIF :::
        println(s"<<< TIF >>>")
        var tifLoadDf: DataFrame = null
        var tifTessDf: DataFrame = null

        try {
            tifLoadDf = spark.read
                .format("gdal")
                .option("pathGlobFilter", "*.TIF")
                .load("src/test/resources/modis")
                .cache()
            val tifLoadCnt = tifLoadDf.count()
            info(s"tif load count? $tifLoadCnt)")
            //info(s"... first -> ${tifLoadDf.first().toSeq.toString()}")
            tifLoadDf.limit(1).show()

            tifTessDf = tifLoadDf
                .withColumn("tiles", rst_tessellate($"tile", 3))
                .withColumn("bbox", st_aswkt(rst_boundingbox($"tile")))
                .select("bbox", "path", "tiles")
                .withColumn("avg", rst_avg($"tiles"))
                .limit(100) // <- keep this smallish!
                .cache()
            val tifTessCnt = tifTessDf.count()
            info(s"tif tess count (expect 100)? $tifTessCnt)")
            FileUtils.deleteDfTilePathDirs(tifLoadDf, verboseLevel = 1, msg = "tifLoadDf") // <- delete + uncache previous phase
            tifTessDf.limit(1).show()

            tifLoadDf
                .createOrReplaceTempView("source")

            noException should be thrownBy spark.sql("""
                                                       |select rst_tessellate(tile, 3) from source
                                                       |""".stripMargin)
            val tifResult = tifTessDf
                .select(explode(col("avg")).alias("a"))
                .groupBy("a")
                .count()
                .collect()
            FileUtils.deleteDfTilePathDirs(tifTessDf, colName = "tiles", verboseLevel = 1, msg = "tifTessDf")
            tifResult.length should be(100) // <- full is 441
            //info(s"tif result example -> ${tifResult.head}")
        } finally {
            // these are uncached in the delete paths call ^
            Try(tifLoadDf.unpersist())
            Try(tifTessDf.unpersist())
        }

        // ::: [2] NETCDF :::
        println(s"<<< NETCDF >>>")
        drivers = new JVector[String]() // java.util.Vector
        drivers.add("netCDF")

        info("\n<<< testing some tessellation + combine steps for Coral Bleaching netcdf >>>")
        info("  - NOTE: GETS FILTERED TO SUBDATASET 'bleaching_alert_area' (1 BAND) -\n")

        filePath = RST_TessellateBehaviors.super.getClass.getResource("/binary/netcdf-coral/").getPath
        info(s"filePath -> $filePath")
        rawPath = s"""${filePath}ct5km_baa-max-7d_v3.1_20220104.nc"""
        info(s"rawPath -> ${rawPath}")

        var netLoadDf: DataFrame = null
        var netTessDf: DataFrame = null
        var netAvgDf: DataFrame = null

        try {
            netLoadDf = spark.read
                .format("gdal")
                .option("subdatasetName", "bleaching_alert_area")
                .load(rawPath)
                .cache()
            val netLoadCnt = netLoadDf.count()
            info(s"netcdf load count? $netLoadCnt")
            //info(s"first -> ${netLoadDf.first().toSeq.toString()}")
            netLoadDf.limit(1).show()

            netTessDf = netLoadDf
                .select(rst_tessellate($"tile", lit(0)).alias("tile"))//, $"band_num")
                .cache()
            val netTessCnt = netTessDf.count()
            FileUtils.deleteDfTilePathDirs(netLoadDf, verboseLevel = 1, msg = "netLoadDf") // <- uncache + delete previous phase
            info(s"netcdf tessellate count? $netTessCnt)")
            //info(s"... first -> ${netTessDf.first().toSeq.toString()}")
            netTessDf.limit(3).show()

            val netResult = netTessDf.collect()
            netResult.length should be(117)

            // additional to mimic raster_to_grid more
            netAvgDf = netTessDf
                .groupBy("tile.index_id")
                .agg(rst_combineavg_agg(col("tile")).alias("tile"))
                .withColumn("grid_measures", rst_avg(col("tile")))
                .select(
                    "grid_measures",
                    "tile"
                )
                .limit(10) // <- keep this smallish!
                .cache()
            val netAvgCnt = netAvgDf.count()
            FileUtils.deleteDfTilePathDirs(netTessDf, verboseLevel = 1, msg = "netTessDf")  // <- uncache + delete previous phase
            info(s"netcdf avg count? ${netAvgCnt}")
            netAvgDf.limit(3).show()

            val validDf = netAvgDf
                .filter(size(col("grid_measures")) > lit(0))
                .select(
                    posexplode(col("grid_measures")).as(Seq("band_id", "measure")),
                    col("tile").getField("index_id").alias("cell_id")
                )
                .select(
                    col("band_id"),
                    col("cell_id"),
                    col("measure")
                )
            val validDfCnt = validDf.count()
            val invalidDf = netAvgDf
                .filter(size(col("grid_measures")) === lit(0))
                .select(
                    lit(0).alias("band_id"),
                    lit(0.0).alias("measure"),
                    col("tile").getField("index_id").alias("cell_id")
                )
                .select(
                    col("band_id"),
                    col("cell_id"),
                    col("measure")
                )
            val invalidDfCnt = invalidDf.count()
            val hasValid = validDfCnt > 0
            info(s"per band measures - valid count? $validDfCnt, invalid count? $invalidDfCnt")
            info(s"validDf count where measure <> 0.0? ${validDf.filter("measure <> 0.0").count()}")
            validDf.filter("measure <> 0.0").limit(5).show()

            FileUtils.deleteDfTilePathDirs(netAvgDf, verboseLevel = 1, msg = "netAvgDf")
        } finally {
            Try(netLoadDf.unpersist())
            Try(netTessDf.unpersist())
            Try(netAvgDf.unpersist())
        }

        //        info("\n<<< testing [[Dataset]] for Coral Bleaching netcdf >>>\n")
        //
        //        ds = gdal.OpenEx(rawPath, GA_ReadOnly, drivers)
        //        ds != null should be(true)
        //        info(s"ds description -> ${ds.GetDescription()}")
        //        info(s"ds rasters -> ${ds.GetRasterCount()}") // <- 0 for this one
        //        info(s"ds files -> ${ds.GetFileList()}")
        //        //info(s"ds tile-1 -> ${ds.GetRasterBand(1).GetDescription()}")
        //
        //        info("\n- testing [[RasterIO.rawPathAsDatasetOpt]] for netcdf coral bleaching -\n")
        //
        //        val ds2 = RasterIO.rawPathAsDatasetOpt(rawPath, subNameOpt = None, driverNameOpt = Some("netCDF"), exprConfigOpt)
        //        ds2.isDefined should be(true)
        //        info(s"ds2 description -> ${ds2.get.GetDescription()}")
        //        info(s"ds2 num rasters -> ${ds2.get.GetRasterCount()}")     // <  0
        //        Try(info(s"ds2 layer count -> ${ds2.get.GetLayerCount()}")) // <- 0
        //        info(s"ds2 files -> ${ds2.get.GetFileList()}")              // <- 1
        //        info(s"ds2 meta domains -> ${ds2.get.GetMetadataDomainList()}")
        //
        //        Try(info(s"<<< ds2 SRS (pre)? ${ds2.get.GetSpatialRef().toString} >>>")) // <- exception
        //        ds2.get.SetSpatialRef(srs4326)
        //        Try(info(s"<<< ds2 SRS (post)? ${ds2.get.GetSpatialRef().toString} >>>")) // <- good


        // ::: [3] ZARR :::
        println(s"<<< ZARR >>>")
        drivers = new JVector[String]() // java.util.Vector
        drivers.add("Zarr")

        info("\n<<< testing tessellation for zarr >>>\n")

        // "src/test/resources/binary..."
        filePath = RST_TessellateBehaviors.super.getClass.getResource("/binary/zarr-example/").getPath
        info(s"zarr filePath -> $filePath")
        rawPath = s"""${filePath}zarr_test_data.zip"""
        info(s"zarr rawPath -> ${rawPath}")

        var zarrLoadDf: DataFrame = null
        var zarrTessDf: DataFrame = null
        try {
            // - zarr doesn't have any SRS, so we have to pass
            //   new arg `skipProject = true` to the RST_Tessellate cmd.
            zarrLoadDf = spark.read
                .format("gdal")
                .option("driverName", "Zarr")
                .option("vsizip", "true")
                .option("subdatasetName", "/group_with_attrs/F_order_array")
                .load(rawPath)
                //.withColumn("tile", rst_separatebands($"tile")) // <- this causes issues
                .cache()
            val zarrLoadCnt = zarrLoadDf.count()
            info(s"zarr load count? $zarrLoadCnt")
            //info(s"... zarr load first -> ${zarrLoadDf.first().toSeq.toString()}")
            zarrLoadDf.limit(1).show()

            zarrTessDf = zarrLoadDf
                .select(rst_tessellate($"tile", lit(0), lit(false)).alias("tile")) // <- skipProject = false (default)
                .cache()
            val zarrTessCnt = zarrTessDf.count()
            FileUtils.deleteDfTilePathDirs(zarrLoadDf, verboseLevel = 1, msg = "zarrLoadDf") // <- uncache + delete previous phase
            info(s"zarr tessellate count? $zarrTessCnt)")
            //info(s"... zarr tessellate first -> ${zarrTessDf.first().toSeq.toString()}")
            zarrTessDf.limit(3).show()

            val zarrTessResult = zarrTessDf.collect()
            zarrTessResult.length should be(5)
            FileUtils.deleteDfTilePathDirs(zarrTessDf, verboseLevel = 1, msg = "zarrTessDf") // <- uncache + delete previous phase

        } finally {
            Try(zarrLoadDf.unpersist())
            Try(zarrTessDf.unpersist())
        }
    }

}
