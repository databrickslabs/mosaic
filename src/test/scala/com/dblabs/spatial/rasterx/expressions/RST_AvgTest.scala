package com.dblabs.spatial.rasterx.expressions

import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.test.SharedSparkSession

class RST_AvgTest extends PlanTest with SharedSparkSession {

    test("Rst_Avg should return the average value per band of the raster") {
        spark.sparkContext.setLogLevel("ERROR")
        import com.dblabs.spatial.rasterx.functions._
        com.dblabs.spatial.rasterx.functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").toString

        val df = spark
            .createDataFrame(
              Seq(
                (tifPath, Map.empty[String, String])
              )
            )
            .toDF("path", "metadata")
            .select(struct("path", "metadata").as("tile"))

        val result = df.select(rst_avg(df("tile")))

        result.show()

    }

}
