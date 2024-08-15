package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.utils.FileUtils
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.scalatest.matchers.should.Matchers.{be, convertToAnyShouldWrapper}

import java.nio.file.{Files, Paths}
import scala.util.Try


trait RST_WriteBehaviors extends QueryTest {

    // noinspection MapGetGet
    def behaviors(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val sc = this.spark
        import sc.implicits._
        sc.sparkContext.setLogLevel("ERROR")

        // init
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(sc)
        import mc.functions._

        val writeDir = "/tmp/mosaic_tmp/write-tile"
        val writeDirJava = Paths.get(writeDir)
        Try(FileUtils.deleteRecursively(writeDir, keepRoot = false))
        Files.createDirectories(writeDirJava)
        Files.list(Paths.get(writeDir)).count() should be (0)

        val rasterDf = spark.read
            .format("binaryFile")
            .option("pathGlobFilter", "*.TIF")
            .load("src/test/resources/modis")
            //.drop("content")

        // test write path tiles (scala for this)
        val gridTiles1 = rasterDf
            .withColumn("tile", rst_maketiles($"path"))
            .filter(!rst_isempty($"tile"))
            .select(rst_write($"tile", writeDir))
            .first()
            .asInstanceOf[GenericRowWithSchema].get(0)

        val createInfo1 = gridTiles1.asInstanceOf[GenericRowWithSchema].getAs[Map[String, String]](2)
        //info(s"createInfo1 -> $createInfo1")
        val path1Java = Paths.get(createInfo1("path"))

        Files.list(path1Java.getParent).count() should be (1)
        Try(FileUtils.deleteRecursively(writeDir, keepRoot = false))
        Files.createDirectories(writeDirJava)
        Files.list(Paths.get(writeDir)).count() should be (0)

        // test write content tiles (sql for this)
        rasterDf.createOrReplaceTempView("source")

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
            .first()
            .asInstanceOf[GenericRowWithSchema].get(0)

        val createInfo2 = gridTilesSQL.asInstanceOf[GenericRowWithSchema].getAs[Map[String, String]](2)
        //info(s"createInfo2 -> $createInfo2")
        val path2Java = Paths.get(createInfo2("path"))

        Files.list(path2Java.getParent).count() should be (1)
        Try(FileUtils.deleteRecursively(writeDir, keepRoot = false))
        Files.createDirectories(writeDirJava)
        Files.list(Paths.get(writeDir)).count() should be (0)
    }

}
