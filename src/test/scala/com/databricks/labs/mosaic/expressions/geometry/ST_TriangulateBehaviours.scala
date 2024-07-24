package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.functions.MosaicRegistryBehaviors.mosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.{array, collect_list, explode, lit}
import org.apache.spark.sql.types._
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers._


trait ST_TriangulateBehaviours extends QueryTest {

    val pointsPath = "src/test/resources/binary/elevation/sd46_dtm_point.shp"
    val linesPath = "src/test/resources/binary/elevation/sd46_dtm_breakline.shp"
    val outputRegion = "POLYGON((348000 462000, 348000 461000, 349000 461000, 349000 462000, 348000 462000))"
    val buffer = 50.0
    val tolerance = 1.0

    def simpleTriangulateBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {

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
            .withColumn("mesh", st_triangulate($"masspoints", $"breaklines", lit(tolerance)))
            .drop($"masspoints")
        noException should be thrownBy result.collect()
        result.count() shouldBe 4435

    }

    def conformingTriangulateBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {

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

        val breaklines = MosaicContext.read
            .option("asWKB", "true")
            .format("multi_read_ogr")
            .load(linesPath)
            .withColumn("filterGeom", st_geomfromwkt(lit(outputRegion)))
            .where(st_intersects($"geom_0", st_buffer($"filterGeom", lit(buffer))))

        val linesDf = breaklines
            .where(st_geometrytype($"geom_0") === "LINESTRING")
            .groupBy()
            .agg(collect_list($"geom_0").as("breaklines"))

        val result = points
            .groupBy()
            .agg(collect_list($"geom_0").as("masspoints"))
            .crossJoin(linesDf)
            .withColumn("mesh", st_triangulate($"masspoints", $"breaklines", lit(tolerance)))
            .drop($"masspoints", $"breaklines")

        noException should be thrownBy result.collect()
        result.count() should be > points.count()

    }

}
