package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.functions.MosaicRegistryBehaviors.mosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.{explode, lit}
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers.{an, be, convertToAnyShouldWrapper}


trait ST_TriangulateBehaviours extends QueryTest {

    def simpleTriangulateBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {

        val mc = mosaicContext
        import mc.functions._
        val sc = spark
        import sc.implicits._
        mc.register(spark)

        val pointsPath = "src/test/resources/binary/elevation/sd46_dtm_point.shp"
        val points = MosaicContext.read.format("multi_read_ogr").load(pointsPath) //spark.read.format("multi_read_ogr").load(pointsPath)
        val result = points
            .limit(10000)
            .groupBy()
            .agg(st_union_agg($"geom_0").as("masspoints"))
            .withColumn("breaklines", st_geomfromwkt(lit("MULTILINESTRING EMPTY")))
            .withColumn("mesh", st_triangulate($"masspoints", $"breaklines", lit(0.01)))
//            .withColumn("masspoints", st_astext(st_updatesrid($"masspoints", lit(27700), lit(4326))))
            .withColumn("mesh", explode($"mesh"))
//            .withColumn("mesh", st_astext(st_updatesrid($"mesh", lit(27700), lit(4326))))
//        result.show(truncate=false)
        noException should be thrownBy result.collect()

    }

}
