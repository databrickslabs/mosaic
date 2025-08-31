package com.dblabs.spatial.rasterx.expressions

import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

class RST_BandMetadataTest extends PlanTest with SharedSparkSession {

    test("Rst_Avg should return the average value per band of the raster") {
        spark.sparkContext.setLogLevel("ERROR")
        import com.dblabs.spatial.rasterx.functions._
        com.dblabs.spatial.rasterx.functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").toString

        val df = spark
            .createDataFrame(
              Seq(
                (1L, tifPath, Map("band" -> "B01", "resolution" -> "500m"))
              )
            )
            .toDF("cell", "path", "metadata")
            .select(struct("cell", "path", "metadata").as("tile"))

        val result = df.select(rst_bandmetadata(df("tile"), lit(1)))

        result.show()

    }

}
