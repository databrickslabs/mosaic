package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.utils.FileUtils
import org.apache.spark.sql.QueryTest
import org.scalatest.matchers.should.Matchers.{be, convertToAnyShouldWrapper}

import java.nio.file.{Files, Paths}
import scala.util.Try


trait RST_WriteBehaviors extends QueryTest {

    // noinspection MapGetGet
    def behaviors(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark

        import mc.functions._
        import sc.implicits._

        val writeDir = "/tmp/mosaic_tmp/write-tile"
        Try(FileUtils.deleteRecursively(writeDir, keepRoot = false))

        val rastersInMemory = spark.read
            .format("binaryFile")
            .option("pathGlobFilter", "*.TIF")
            .load("src/test/resources/modis")
            //.drop("content")

        // test write path tiles (scala for this)
        val gridTiles1 = rastersInMemory
            .withColumn("tile", rst_maketiles($"path"))
            .filter(!rst_isempty($"tile"))
            .select(rst_write($"tile", writeDir))
            .write
                .format("noop")
                .mode("overwrite")
            .save()

        Files.list(Paths.get(writeDir)).count() should be (7)
        Try(FileUtils.deleteRecursively(writeDir, keepRoot = false))

        // test write content tiles (sql for this)
        rastersInMemory.createOrReplaceTempView("source")

        val gridTilesSQL = spark
            .sql(
                s"""
                  |with subquery as (
                  |   select rst_maketiles(content, 'GTiff', -1) as tile from source
                  |)
                  |select rst_write(tile, '$writeDir') as result
                  |from subquery
                  |where not rst_isempty(tile)
                  |""".stripMargin)
            .write
                .format("noop")
                .mode("overwrite")
            .save()

        Files.list(Paths.get(writeDir)).count() should be (7)
        Try(FileUtils.deleteRecursively(writeDir, keepRoot = false))
    }

}
