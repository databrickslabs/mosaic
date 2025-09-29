package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.functions
import com.databricks.labs.gbx.udfs.st_buffer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._

class RST_AggEvalTest extends PlanTest with SilentSparkSession {

    test("RST_AggEvalTest should evaluate expressions on raster columns") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val pyfunc = "def myfunc(x):\n  return x * 2"

        def runQuery(df: DataFrame): Unit = {
            df
                .withColumn("bbox", rst_boundingbox(col("raster")))
                .withColumn("clipper", st_buffer(col("bbox"), lit(-500000.0))) // projection in meters 1 px is ~470m
                .withColumn("raster", rst_clip(col("raster"), col("clipper"), lit(true)))
                .groupBy(lit(1))
                .agg(
                  rst_combineavgagg(col("raster")),
                  rst_derivedbandagg(col("raster"), pyfunc, "myfunc"),
                  rst_mergeagg(col("raster"))
                )
                .collect()
        }

        val df: DataFrame = Seq(
          (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"),
          (2, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B02.TIF"),
          (3, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B03.TIF")
        ).toDF("id", "path")
            .withColumn("raster", rst_fromfile(col("path"), lit("GTiff")))

        noException should be thrownBy runQuery(df)

        val df2 = spark.read
            .format("binaryFile")
            .load(tifPath)
            .withColumn("raster", rst_fromcontent(col("content"), lit("GTiff")))

        noException should be thrownBy runQuery(df2)

    }

}
