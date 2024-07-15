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

    def simpleTriangulateBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {

        val mc = mosaicContext
        import mc.functions._
        val sc = spark
        import sc.implicits._
        mc.register(spark)

        val pointsPath = "src/test/resources/binary/elevation/sd46_dtm_point.shp"
        val points = MosaicContext.read.option("asWKB", "true").format("multi_read_ogr").load(pointsPath) //spark.read.format("multi_read_ogr").load(pointsPath)
        val result = points
            .groupBy()
            .agg(collect_list($"geom_0").as("masspoints"))
            .withColumn("breaklines", array().cast(ArrayType(StringType)))
            .withColumn("mesh", st_triangulate($"masspoints", $"breaklines", lit(0.01)))
            .drop($"masspoints")
        noException should be thrownBy result.collect()
        result.count() shouldBe 193068L +- 100 //compare to result from QGIS / GEOS

    }

}
