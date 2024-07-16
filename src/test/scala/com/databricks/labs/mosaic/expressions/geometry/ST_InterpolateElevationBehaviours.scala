package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.functions.MosaicRegistryBehaviors.mosaicContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.types._
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers._

trait ST_InterpolateElevationBehaviours extends QueryTest {

    def simpleInterpolationBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {

        val mc = mosaicContext
        import mc.functions._
        val sc = spark
        import sc.implicits._
        mc.register(spark)

        val pointsPath = "src/test/resources/binary/elevation/sd46_dtm_point.shp"
        val points = MosaicContext.read.option("asWKB", "true").format("multi_read_ogr").load(pointsPath)
        val result = points
            .groupBy()
            .agg(collect_list($"geom_0").as("masspoints"))
            .withColumn("breaklines", array().cast(ArrayType(StringType)))
            .withColumn("tolerance", lit(0.01))
            .withColumn("origin", st_point(lit(348000.0), lit(462000.0)))
            .withColumn("grid_size_x", lit(1000))
            .withColumn("grid_size_y", lit(1000))
            .withColumn("pixel_size_x", lit(1.0))
            .withColumn("pixel_size_y", lit(-1.0))
            .withColumn("elevation", st_interpolateelevation(
                $"masspoints", $"breaklines", lit(0.01),
                $"origin", $"grid_size_x", $"grid_size_y",
                $"pixel_size_x", $"pixel_size_y"))
            .drop(
                $"masspoints", $"breaklines", $"tolerance", $"origin",
                $"grid_size_x", $"grid_size_y", $"pixel_size_x", $"pixel_size_y"
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

        val pointsPath = "src/test/resources/binary/elevation/sd46_dtm_point.shp"
        val linesPath = "src/test/resources/binary/elevation/sd46_dtm_breakline.shp"

        val points = MosaicContext.read.option("asWKB", "true").format("multi_read_ogr").load(pointsPath)
        val breaklines = MosaicContext.read.option("asWKB", "true").format("multi_read_ogr").load(linesPath)

        val linesDf = breaklines
            .where(st_geometrytype($"geom_0") === "LINESTRING")
            .groupBy()
            .agg(collect_list($"geom_0").as("breaklines"))

        val result = points
            .groupBy()
            .agg(collect_list($"geom_0").as("masspoints"))
            .crossJoin(linesDf)
            .withColumn("tolerance", lit(0.01))
            .withColumn("origin", st_point(lit(348000.0), lit(462000.0)))
            .withColumn("grid_size_x", lit(1000))
            .withColumn("grid_size_y", lit(1000))
            .withColumn("pixel_size_x", lit(1.0))
            .withColumn("pixel_size_y", lit(-1.0))
            .withColumn("interpolated_grid_point", st_interpolateelevation(
                $"masspoints", $"breaklines", lit(0.01),
                $"origin", $"grid_size_x", $"grid_size_y",
                $"pixel_size_x", $"pixel_size_y"))
            .withColumn("elevation", st_z($"interpolated_grid_point"))
            .drop(
                $"masspoints", $"breaklines", $"tolerance", $"origin",
                $"grid_size_x", $"grid_size_y", $"pixel_size_x", $"pixel_size_y"
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
