package com.databricks.labs.gbx.rasterx.ds

import com.databricks.labs.gbx.rasterx
import com.databricks.labs.gbx.rasterx.functions
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.gdal.gdal.gdal
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.ListHasAsScala

class GDAL_DataSourceTest extends PlanTest with SilentSparkSession {

    test("GDAL Data Source must read tif files") {
        import com.databricks.labs.gbx.rasterx.functions._
        rasterx.functions.register(spark)
        val sp = spark
        import sp.implicits._

        val tifPath = this.getClass.getResource("/modis/").toString

        val res = spark.read
            .format("gdal")
            .option("sizeInMB", "1")
            .load(tifPath)
            .limit(10)
            .select(
              rst_avg(col("tile")).alias("avg")
            )
            .as[Array[Double]]
            .collect()

        res.foreach(arr => arr.foreach(v => v should be >= 0.0))

    }

    test("GDAL Data Source must write valid tifs for rows") {
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val df = spark.read
            .format("gdal")
            .option("sizeInMB", "1")
            .load(tifPath)

        df.write
            .format("gdal")
            .option("ext", "tif")
            .mode("append")
            .save("/tmp/gdal_test_out")

        val outPath = Paths.get("/tmp/gdal_test_out")

        val testFile = Files.list(outPath).filter(p => !p.toString.contains(".crc")).limit(1).toList.get(0)

        val ds = gdal.Open(testFile.toString)
        ds.GetRasterBand(1).AsMDArray().GetStatistics().getValid_count should be >= 0L

        val dss = Files.list(outPath).filter(p => !p.toString.contains(".crc")).toList.asScala.map(p => gdal.Open(p.toString)).toList
//        dss.foreach { ds =>
//            println("_".repeat(64))
//            RasterDebuger.printColorGridDenseTruecolor(ds, 128, 128)
//        }
//  RasterDebuger.printColorGridDenseTruecolor(dss.head, 128, 128)

//        while (true) {}

        Files.list(outPath).toList.asScala.foreach(Files.deleteIfExists)
        Files.deleteIfExists(outPath)

        //        while (true) {}

    }

}
