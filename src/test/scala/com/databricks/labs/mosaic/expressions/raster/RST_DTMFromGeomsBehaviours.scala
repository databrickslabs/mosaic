package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.types.model.TriangulationSplitPointTypeEnum
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.functions.{array, collect_list, lit}
import org.apache.spark.sql.test.SharedSparkSessionGDAL
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.scalatest.matchers.should.Matchers._

trait RST_DTMFromGeomsBehaviours extends SharedSparkSessionGDAL {

    val pointsPath = "src/test/resources/binary/elevation/sd46_dtm_point.shp"
    val linesPath = "src/test/resources/binary/elevation/sd46_dtm_breakline.shp"
    val mergeTolerance = 1e-6
    val snapTolerance = 0.01
    val splitPointFinder = TriangulationSplitPointTypeEnum.NONENCROACHING
    val noData = -9999.0

    def simpleRasterizeTest(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {

        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        val sc = spark
        import sc.implicits._
        mc.register(spark)

        val pointsDf = MosaicContext.read
            .option("asWKB", "true")
            .format("multi_read_ogr")
            .load(pointsPath)
        val result = pointsDf
            .withColumn("geom_0", st_geomfromwkb($"geom_0"))
            .withColumn("geom_0", st_setsrid($"geom_0", lit(27700)))
            .groupBy()
            .agg(collect_list($"geom_0").as("masspoints"))
            .withColumn("breaklines", array().cast(ArrayType(StringType)))
            .withColumn("mergeTolerance", lit(mergeTolerance))
            .withColumn("snapTolerance", lit(snapTolerance))
            .withColumn("splitPointFinder", lit(splitPointFinder.toString))
            .withColumn("origin", st_point(lit(348000.0), lit(462000.0)))
            .withColumn("grid_size_x", lit(1000))
            .withColumn("grid_size_y", lit(1000))
            .withColumn("pixel_size_x", lit(1.0))
            .withColumn("pixel_size_y", lit(-1.0))
            .withColumn("noData", lit(noData))
            .withColumn("tile", rst_dtmfromgeoms(
                $"masspoints", $"breaklines", $"mergeTolerance", $"snapTolerance", $"splitPointFinder",
                $"origin", $"grid_size_x", $"grid_size_y",
                $"pixel_size_x", $"pixel_size_y", $"noData"))
            .drop(
                $"masspoints", $"breaklines", $"mergeTolerance", $"snapTolerance", $"splitPointFinder",
                $"origin", $"grid_size_x", $"grid_size_y",
                $"pixel_size_x", $"pixel_size_y", $"noData"
            ).cache()
        noException should be thrownBy result.collect()

    }

    def conformedTriangulationRasterizeTest(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {

        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        val sc = spark
        import sc.implicits._
        mc.register(spark)

        val outputRegion = "POLYGON((348000 462000, 348000 461000, 349000 461000, 349000 462000, 348000 462000))"

        val pointsDf = MosaicContext.read
            .option("asWKB", "true")
            .format("multi_read_ogr")
            .load(pointsPath)

        val breaklines = MosaicContext.read.option("asWKB", "true").format("multi_read_ogr").load(linesPath)

        val linesDf = breaklines
            .where(st_geometrytype($"geom_0") === "LINESTRING")
            .withColumn("filterGeom", st_geomfromwkt(lit(outputRegion)))
            .where(st_intersects($"geom_0", st_buffer($"filterGeom", lit(500.0))))
            .groupBy()
            .agg(collect_list($"geom_0").as("breaklines"))

        val result = pointsDf
            .withColumn("geom_0", st_geomfromwkb($"geom_0"))
            .withColumn("geom_0", st_setsrid($"geom_0", lit(27700)))
            .withColumn("filterGeom", st_geomfromwkt(lit(outputRegion)))
            .where(st_intersects($"geom_0", st_buffer($"filterGeom", lit(500.0))))
            .where(st_x($"geom_0").notEqual(lit(0)))
            .where(st_y($"geom_0").notEqual(lit(0)))
            .groupBy()
            .agg(collect_list($"geom_0").as("masspoints"))
            .crossJoin(linesDf)
            .withColumn("mergeTolerance", lit(mergeTolerance))
            .withColumn("snapTolerance", lit(snapTolerance))
            .withColumn("splitPointFinder", lit(splitPointFinder.toString))
            .withColumn("origin", st_point(lit(348000.0), lit(462000.0)))
            .withColumn("grid_size_x", lit(1000))
            .withColumn("grid_size_y", lit(1000))
            .withColumn("pixel_size_x", lit(1.0))
            .withColumn("pixel_size_y", lit(-1.0))
            .withColumn("noData", lit(noData))
            .withColumn("tile", rst_dtmfromgeoms(
                $"masspoints", $"breaklines", $"mergeTolerance", $"snapTolerance", $"splitPointFinder",
                $"origin", $"grid_size_x", $"grid_size_y",
                $"pixel_size_x", $"pixel_size_y", $"noData"))
            .drop(
                $"masspoints", $"breaklines", $"mergeTolerance", $"snapTolerance", $"splitPointFinder",
                $"origin", $"grid_size_x", $"grid_size_y",
                $"pixel_size_x", $"pixel_size_y", $"noData"
            ).cache()
        noException should be thrownBy result.collect()
    }

    def multiRegionTriangulationRasterizeTest(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {

        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        val sc = spark
        import sc.implicits._
        mc.register(spark)

        val outputRegion = "POLYGON ((340000 460000, 350000 460000, 350000 470000, 340000 470000, 340000 460000))"
        val rasterBuffer = 500.0

        val rasterExtentsDf =
            List(outputRegion).toDF("wkt")
            .withColumn("extent_geom", st_geomfromwkt($"wkt"))
            .withColumn("extent_geom", st_setsrid($"extent_geom", lit(27700)))
            .withColumn("cells", grid_tessellateexplode($"extent_geom", lit(3)))
            .withColumn("extent", st_geomfromwkb($"cells.wkb"))
            .withColumn("extent_buffered", st_buffer($"extent", lit(rasterBuffer)))
            .withColumn("raster_origin", st_point(st_xmin($"extent"), st_ymax($"extent"))) // top left
            .withColumn("raster_origin", st_setsrid($"raster_origin", lit(27700)))
            .select("cells.index_id", "extent_buffered", "raster_origin")

        val pointsDf = MosaicContext.read
            .option("asWKB", "true")
            .format("multi_read_ogr")
            .load(pointsPath)
            .where($"geom_0".notEqual(lit("POINT EMPTY")))
            .withColumn("geom", st_geomfromwkb($"geom_0"))
            .withColumn("geom", st_setsrid($"geom", lit(27700)))
            .crossJoin(rasterExtentsDf)
            .where(st_intersects($"geom", $"extent_buffered"))
            .groupBy("index_id", "raster_origin")
            .agg(collect_list($"geom").as("masspoints"))

        val linesDf = MosaicContext.read
            .option("asWKB", "true")
            .format("multi_read_ogr")
            .load(linesPath)
            .where(st_geometrytype($"geom_0") === "LINESTRING")
            .withColumn("geom", st_geomfromwkb($"geom_0"))
            .withColumn("geom", st_setsrid($"geom", lit(27700)))
            .crossJoin(rasterExtentsDf)
            .where(st_intersects($"geom", $"extent_buffered"))
            .groupBy("index_id", "raster_origin")
            .agg(collect_list($"geom").as("breaklines"))

        val inputsDf = pointsDf
            .join(linesDf, Seq("index_id", "raster_origin"), "left")
            .withColumn("index_id", $"index_id".cast(StringType))

        val result = inputsDf
            .repartition(sc.sparkContext.defaultParallelism)
            .withColumn("mergeTolerance", lit(mergeTolerance))
            .withColumn("snapTolerance", lit(snapTolerance))
            .withColumn("splitPointFinder", lit(splitPointFinder.toString))
            .withColumn("grid_size_x", lit(1000))
            .withColumn("grid_size_y", lit(1000))
            .withColumn("pixel_size_x", lit(1.0))
            .withColumn("pixel_size_y", lit(-1.0))
            .withColumn("noData", lit(noData))
            .withColumn("tile", rst_dtmfromgeoms(
                $"masspoints", $"breaklines", $"mergeTolerance", $"snapTolerance", $"splitPointFinder",
                $"raster_origin", $"grid_size_x", $"grid_size_y",
                $"pixel_size_x", $"pixel_size_y", $"noData"))
            .drop(
                $"masspoints", $"breaklines", $"mergeTolerance", $"snapTolerance", $"splitPointFinder",
                $"raster_origin", $"grid_size_x", $"grid_size_y",
                $"pixel_size_x", $"pixel_size_y", $"noData"
            ).cache()
        noException should be thrownBy result.collect()
        result.count() shouldBe inputsDf.count()
    }

}
