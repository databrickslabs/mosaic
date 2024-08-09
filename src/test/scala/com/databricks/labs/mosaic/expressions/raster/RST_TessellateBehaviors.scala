package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers._

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

        // ::: [1] TIF :::
        val rastersInMemory = spark.read
            .format("gdal")
            .option("pathGlobFilter", "*.TIF")
            .load("src/test/resources/modis")
        //info(s"rastersInMemory -> ${rastersInMemory.first().toSeq.toString()}")

        val gridTiles = rastersInMemory
            .withColumn("tiles", rst_tessellate($"tile", 3))
            .withColumn("bbox", st_aswkt(rst_boundingbox($"tile")))
            .select("bbox", "path", "tiles")
            .withColumn("avg", rst_avg($"tiles"))
        //info(s"gridTiles -> ${gridTiles.first().toSeq.toString()}")

        rastersInMemory
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_tessellate(tile, 3) from source
                                                   |""".stripMargin)

        noException should be thrownBy rastersInMemory
            .withColumn("tiles", rst_tessellate($"tile", 3))
            .select("tiles")

        val result = gridTiles.select(explode(col("avg")).alias("a")).groupBy("a").count().collect()

        result.length should be(441)
        //info(s"tif example -> ${result.head}")

        // ::: [2] NETCDF :::
        val netcdf = spark.read
            .format("gdal")
            .load("src/test/resources/binary/netcdf-CMIP5/prAdjust_day_HadGEM2-CC_SMHI-DBSrev930-GFD-1981-2010-postproc_rcp45_r1i1p1_20201201-20201231.nc")
            .withColumn("tile", rst_separatebands($"tile"))
            .withColumn("tile", rst_setsrid($"tile", lit(4326))) // <- this seems to be required
            .limit(1)

        info(s"netcdf count? ${netcdf.count()}")

        val netcdfGridTiles = netcdf
            .select(rst_tessellate($"tile", lit(1)).alias("tile"))

        val netcdfResult = netcdfGridTiles.collect()

        netcdfResult.length should be(491)
        //info(s"netcd example -> ${netcdfResult.head}")
        //netcdfGridTiles.limit(3).show()

        // ::: [3] ZARR :::
        // - zarr doesn't have any SRS, so we have to pass
        //   new arg `skipProject = true` to the RST_Tessellate cmd.
        val zarrDf = spark.read
            .format("gdal")
                .option("driverName", "Zarr")
                .option("vsizip", "true")
                .option("subdatasetName", "/group_with_attrs/F_order_array")
            .load("src/test/resources/binary/zarr-example/")
            .limit(1)

        info(s"zarr count? ${zarrDf.count()}")

        val zarrGridDf = zarrDf
            .select(rst_tessellate($"tile", lit(0), lit(true)).alias("tile")) // <- skipProject = true

        val zarrResult = zarrGridDf.collect()

        zarrResult.length should be(5)
        //info(s"zarr example -> ${zarrResult.head}")
    }

}
