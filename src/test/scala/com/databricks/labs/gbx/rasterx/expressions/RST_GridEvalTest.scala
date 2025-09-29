package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.functions
import com.databricks.labs.gbx.udfs.st_buffer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._

class RST_GridEvalTest extends PlanTest with SilentSparkSession {

    test("RST_GridEval should evaluate expressions on raster columns") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        def runQuery(df: DataFrame): Unit = {
            val base = df
                .withColumn("bbox", rst_boundingbox(col("raster")))
                .withColumn("clipper", st_buffer(col("bbox"), lit(-500000.0))) // projection in meters 1 px is ~470m
                .withColumn("raster", rst_clip(col("raster"), col("clipper"), lit(true)))
                .cache()
            base.collect()
            Seq(
              rst_h3_rastertogridavg(col("raster"), lit(2)),
              rst_h3_rastertogridcount(col("raster"), lit(2)),
              rst_h3_rastertogridmax(col("raster"), lit(2)),
              rst_h3_rastertogridmin(col("raster"), lit(2)),
              rst_h3_rastertogridmedian(col("raster"), lit(2))
            ).foreach(f => base.select(f).collect())
            base.unpersist()
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

        // noException should be thrownBy

        runQuery(df2)

    }

}
