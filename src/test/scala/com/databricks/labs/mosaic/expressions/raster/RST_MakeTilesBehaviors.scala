package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.scalatest.matchers.should.Matchers._

trait RST_MakeTilesBehaviors extends QueryTest {

    // noinspection MapGetGet
    def behaviors(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val sc = this.spark
        import sc.implicits._
        sc.sparkContext.setLogLevel("ERROR")

        // init
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(sc)
        import mc.functions._

        val rasterDf = spark.read
            .format("binaryFile")
            .option("pathGlobFilter", "*.TIF")
            .load("src/test/resources/modis")

        val gridTiles1 = rasterDf
            .withColumn("tile", rst_maketiles($"content", "GTiff", -1))
            .select(!rst_isempty($"tile"))
            .as[Boolean]
            .collect()

        gridTiles1.forall(identity) should be(true)

        rasterDf.createOrReplaceTempView("source")

        val gridTilesSQL = spark
            .sql("""
                   |with subquery as (
                   |   select rst_maketiles(content, 'GTiff', -1) as tile from source
                   |)
                   |select not rst_isempty(tile) as result
                   |from subquery
                   |""".stripMargin)
            .as[Boolean]
            .collect()

        gridTilesSQL.forall(identity) should be(true)


        val gridTilesSQL2 = spark
            .sql(
                """
                  |with subquery as (
                  |   select rst_maketiles(content, 'GTiff', 4) as tile from source
                  |)
                  |select not rst_isempty(tile) as result
                  |from subquery
                  |""".stripMargin)
            .as[Boolean]
            .collect()

        gridTilesSQL2.forall(identity) should be(true)

        val gridTilesSQL3 = spark
            .sql(
                """
                  |with subquery as (
                  |   select rst_maketiles(path, 'GTiff', 4) as tile from source
                  |)
                  |select not rst_isempty(tile) as result
                  |from subquery
                  |""".stripMargin)
            .as[Boolean]
            .collect()

        gridTilesSQL3.forall(identity) should be(true)

        val gridTilesSQL4 = spark
            .sql(
                """
                  |with subquery as (
                  |   select rst_maketiles(path, 'GTiff', 4, true) as tile from source
                  |)
                  |select not rst_isempty(tile) as result
                  |from subquery
                  |""".stripMargin)
            .as[Boolean]
            .collect()

        gridTilesSQL4.forall(identity) should be(true)

        val gridTiles2 = rasterDf
            .withColumn("tile", rst_maketiles($"path"))
            .select(!rst_isempty($"tile"))
            .as[Boolean]
            .collect()
        
        gridTiles2.forall(identity) should be(true)

    }

}
