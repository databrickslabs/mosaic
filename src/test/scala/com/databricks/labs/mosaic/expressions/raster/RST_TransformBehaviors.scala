package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers._

trait RST_TransformBehaviors extends QueryTest {

    // noinspection MapGetGet
    def behavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val sc = this.spark
        import sc.implicits._
        sc.sparkContext.setLogLevel("ERROR")

        // init
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(sc)
        import mc.functions._

        val rastersInMemory = spark.read
            .format("gdal")
            .option("pathGlobFilter", "*.TIF")
            .load("src/test/resources/modis")

        val gridTiles = rastersInMemory
            .withColumn("tile", rst_transform($"tile", lit(27700)))
            .withColumn("bbox", st_aswkt(rst_boundingbox($"tile")))
            .select("bbox", "path", "tile")
            .withColumn("avg", rst_avg($"tile"))

        rastersInMemory
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_transform(tile, 27700) from source
                                                   |""".stripMargin)

        noException should be thrownBy rastersInMemory
            .withColumn("tile", rst_transform($"tile", lit(27700)))
            .select("tile")

        val result = gridTiles.select(explode(col("avg")).alias("a")).groupBy("a").count().collect()

        result.length should be(7)

    }

}
