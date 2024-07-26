package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.functions.MosaicRegistryBehaviors.mosaicContext
import org.apache.spark.sql.functions.{array, collect_list, lit}
import org.apache.spark.sql.test.SharedSparkSessionGDAL
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.scalatest.matchers.must.Matchers._

trait RST_DTMFromGeomsBehaviours extends SharedSparkSessionGDAL {

    def simpleRasterizeTest(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {

        val mc = mosaicContext
        import mc.functions._
        val sc = spark
        import sc.implicits._
        mc.register(spark)

        val pointsPath = "src/test/resources/binary/elevation/sd46_dtm_point.shp"
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
            .withColumn("tolerance", lit(1.0))
            .withColumn("origin", st_point(lit(348000.0), lit(462000.0)))
            .withColumn("grid_size_x", lit(1000))
            .withColumn("grid_size_y", lit(1000))
            .withColumn("pixel_size_x", lit(1.0))
            .withColumn("pixel_size_y", lit(-1.0))
            .withColumn("tile", rst_dtmfromgeoms(
                $"masspoints", $"breaklines", $"tolerance",
                $"origin", $"grid_size_x", $"grid_size_y",
                $"pixel_size_x", $"pixel_size_y"))
            .drop(
                $"masspoints", $"breaklines", $"tolerance", $"origin",
                $"grid_size_x", $"grid_size_y", $"pixel_size_x", $"pixel_size_y"
            ).cache()
        noException should be thrownBy result.collect()

    }

    def conformedTriangulationRasterizeTest(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {

        val mc = mosaicContext
        import mc.functions._
        val sc = spark
        import sc.implicits._
        mc.register(spark)

        val pointsPath = "src/test/resources/binary/elevation/sd46_dtm_point.shp"
        val linesPath = "src/test/resources/binary/elevation/sd46_dtm_breakline.shp"
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
            .withColumn("tolerance", lit(0.5))
            .withColumn("origin", st_point(lit(348000.0), lit(462000.0)))
            .withColumn("grid_size_x", lit(1000))
            .withColumn("grid_size_y", lit(1000))
            .withColumn("pixel_size_x", lit(1.0))
            .withColumn("pixel_size_y", lit(-1.0))
            .withColumn("tile", rst_dtmfromgeoms(
                $"masspoints", $"breaklines", $"tolerance",
                $"origin", $"grid_size_x", $"grid_size_y",
                $"pixel_size_x", $"pixel_size_y"))
            .drop(
                $"masspoints", $"breaklines", $"tolerance", $"origin",
                $"grid_size_x", $"grid_size_y", $"pixel_size_x", $"pixel_size_y"
            ).cache()
        noException should be thrownBy result.collect()
    }
}
