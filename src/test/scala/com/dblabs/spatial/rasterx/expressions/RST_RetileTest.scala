package com.dblabs.spatial.rasterx.expressions

import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

class RST_RetileTest extends PlanTest with SharedSparkSession {

//    test("Rst_ReTile should return the average value per band of the raster") {
//        spark.sparkContext.setLogLevel("ERROR")
//        import com.dblabs.spatial.rasterx.functions._
//        import com.dblabs.spatial.udfs._
//        com.dblabs.spatial.rasterx.functions.register(spark)
//
//        val tifPath = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").toString
//
//        val df = spark
//            .createDataFrame(
//              Seq((1, tifPath))
//            )
//            .toDF("id", "path")
//            .select(rst_fromfile(col("path"), lit("GTiff")).as("tile"))
//            .withColumn("tile", rst_retile(col("tile"), lit(1000), lit(1000)))
//
//        df.show()
//
//        df.limit(1).show(truncate = false)
//
//        val result = df
//            .select(rst_avg(col("tile")).alias("avg"))
//
//        result.show(truncate = false)
//
//    }

    test("Rst_ReTile should return the average value per band of the raster - binary") {
        spark.sparkContext.setLogLevel("ERROR")
        import com.dblabs.spatial.rasterx.functions._
        import com.dblabs.spatial.udfs._
        com.dblabs.spatial.rasterx.functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").toString

        val df = spark
            .read.format("binaryFile")
            .option("pathGlobFilter", "*.TIF")
            .load(tifPath)
            .select(rst_fromcontent(col("content"), lit("GTiff")).as("tile"))
            .withColumn("tile", rst_retile(col("tile"), lit(1000), lit(1000)))

        df.show()

        df.limit(1).show()

        val result = df
            .select(rst_avg(col("tile")).alias("avg"))

        result.show()

    }

}
