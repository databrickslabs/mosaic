package com.dblabs.spatial.rasterx.expressions

import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

class RST_ClipTest extends PlanTest with SharedSparkSession {

    test("Rst_Avg should return the average value per band of the raster") {
        spark.sparkContext.setLogLevel("ERROR")
        import com.dblabs.spatial.rasterx.functions._
        import com.dblabs.spatial.udfs._
        com.dblabs.spatial.rasterx.functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").toString

        val df = spark
            .createDataFrame(
              Seq((1, tifPath))
            )
            .toDF("id", "path")
            .select(rst_fromfile(col("path"), lit("GTiff")).as("tile"))
            .withColumn("clip", rst_boundingbox(col("tile")))
            .withColumn("clip", st_buffer(col("clip"), lit(-500)))

        val result = df
            .select(rst_clip(col("tile"), col("clip"), lit(true)).alias("clipped_tile"))
            .select(rst_avg(col("clipped_tile")).alias("avg"))

        result.show()

    }

}
