package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.types.model.TriangulationSplitPointTypeEnum
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.functions.MosaicRegistryBehaviors.mosaicContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.types._
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers._

trait ST_InterpolateElevationBehaviours extends QueryTest {

    val pointsPath = "src/test/resources/binary/elevation/sd46_dtm_point.shp"
    val linesPath = "src/test/resources/binary/elevation/sd46_dtm_breakline.shp"
    val outputRegion = "POLYGON((348000 462000, 348000 461000, 349000 461000, 349000 462000, 348000 462000))"
    val buffer = 50.0
    val xWidth = 1000
    val yWidth = 1000
    val xSize = 1.0
    val ySize = -1.0
    val mergeTolerance = 0.0
    val snapTolerance = 0.01
    val splitPointFinder = TriangulationSplitPointTypeEnum.NONENCROACHING
    val origin = "POINT(348000 462000)"

    def simpleInterpolationBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {

        val mc = mosaicContext
        import mc.functions._
        val sc = spark
        import sc.implicits._
        mc.register(spark)

        val points = MosaicContext.read
            .option("asWKB", "true")
            .format("multi_read_ogr")
            .load(pointsPath)
            .withColumn("filterGeom", st_geomfromwkt(lit(outputRegion)))
            .where(st_intersects($"geom_0", st_buffer($"filterGeom", lit(buffer))))

        val result = points
            .groupBy()
            .agg(collect_list($"geom_0").as("masspoints"))
            .withColumn("breaklines", array().cast(ArrayType(StringType)))
            .withColumn("mergeTolerance", lit(mergeTolerance))
            .withColumn("snapTolerance", lit(snapTolerance))
            .withColumn("splitPointFinder", lit(splitPointFinder.toString))
            .withColumn("origin", st_geomfromwkt(lit(origin)))
            .withColumn("grid_size_x", lit(xWidth))
            .withColumn("grid_size_y", lit(yWidth))
            .withColumn("pixel_size_x", lit(xSize))
            .withColumn("pixel_size_y", lit(ySize))
            .withColumn("elevation", st_interpolateelevation(
                $"masspoints", $"breaklines",
                $"mergeTolerance", $"snapTolerance", $"splitPointFinder",
                $"origin", $"grid_size_x", $"grid_size_y",
                $"pixel_size_x", $"pixel_size_y"))
            .drop(
                $"masspoints", $"breaklines",
                $"mergeTolerance", $"snapTolerance", $"splitPointFinder",
                $"origin", $"grid_size_x", $"grid_size_y",
                $"pixel_size_x", $"pixel_size_y"
            )
        noException should be thrownBy result.collect()
        result.count() shouldBe 1000000L
    }

    def conformingInterpolationBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {

        val mc = mosaicContext
        import mc.functions._
        val sc = spark
        import sc.implicits._
        mc.register(spark)

        val points = MosaicContext.read
            .option("asWKB", "true")
            .format("multi_read_ogr")
            .load(pointsPath)
            .withColumn("filterGeom", st_geomfromwkt(lit(outputRegion)))
            .where(st_intersects($"geom_0", st_buffer($"filterGeom", lit(buffer))))

        val linesDf = MosaicContext.read
            .option("asWKB", "true")
            .format("multi_read_ogr")
            .load(linesPath)
            .where(st_geometrytype($"geom_0") === "LINESTRING")
            .withColumn("filterGeom", st_geomfromwkt(lit(outputRegion)))
            .where(st_intersects($"geom_0", st_buffer($"filterGeom", lit(buffer))))
            .groupBy()
            .agg(collect_list($"geom_0").as("breaklines"))

        val result = points
            .groupBy()
            .agg(collect_list($"geom_0").as("masspoints"))
            .crossJoin(linesDf)
            .withColumn("mergeTolerance", lit(mergeTolerance))
            .withColumn("snapTolerance", lit(snapTolerance))
            .withColumn("splitPointFinder", lit(splitPointFinder.toString))
            .withColumn("origin", st_geomfromwkt(lit(origin)))
            .withColumn("grid_size_x", lit(xWidth))
            .withColumn("grid_size_y", lit(yWidth))
            .withColumn("pixel_size_x", lit(xSize))
            .withColumn("pixel_size_y", lit(ySize))
            .withColumn("interpolated_grid_point", st_interpolateelevation(
                $"masspoints", $"breaklines",
                $"mergeTolerance", $"snapTolerance", $"splitPointFinder",
                $"origin", $"grid_size_x", $"grid_size_y",
                $"pixel_size_x", $"pixel_size_y"))
            .withColumn("elevation", st_z($"interpolated_grid_point"))
            .drop(
                $"masspoints", $"breaklines",
                $"mergeTolerance", $"snapTolerance", $"splitPointFinder",
                $"origin", $"grid_size_x", $"grid_size_y",
                $"pixel_size_x", $"pixel_size_y"
            )
            .cache()
        noException should be thrownBy result.collect()
        result.count() shouldBe 1000000L
        val  targetRow = result
            .orderBy(
                st_y($"interpolated_grid_point").asc,
                st_x($"interpolated_grid_point").desc
            )
            .first()
        targetRow.getAs[Double]("elevation") shouldBe 63.55 +- 0.1 // rough reckoning from examining the reference raster
    }

}
