package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.functions
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._

class RST_GeneratorsEvalTest extends PlanTest with SilentSparkSession {

    test("RST_GeneratorsEval should evaluate expressions on raster columns") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)
        val tifPath = this.getClass.getResource("/modis/").toString

        val df: DataFrame = Seq(
          (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"),
          (2, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B02.TIF"),
          (3, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B03.TIF")
        ).toDF("id", "path")
            .withColumn("raster", rst_fromfile(col("path"), lit("GTiff")))

        // Generators should be kept separately to avoid long execution times
        // as each generates rows it is a multiplier on the number of input rows

        noException should be thrownBy df.withColumn("rst_h3_tessellate", rst_h3_tessellate(col("raster"), lit(1))).collect()

        noException should be thrownBy df.withColumn("rst_maketiles", rst_maketiles(col("raster"), lit(1000), lit(1000))).collect()

        noException should be thrownBy df.withColumn("rst_retile", rst_retile(col("raster"), lit(1000), lit(1000))).collect()

        noException should be thrownBy
            df.withColumn("rst_tooverlappingtiles", rst_tooverlappingtiles(col("raster"), lit(1000), lit(1000), lit(10))).collect()

        noException should be thrownBy df.withColumn("separated", rst_separatebands(col("raster"))).collect()

        val df2 = spark.read.format("binaryFile").load(tifPath)
            .withColumn("raster", rst_fromcontent(col("content"), lit("GTiff")))

        noException should be thrownBy df2.withColumn("rst_h3_tessellate", rst_h3_tessellate(col("raster"), lit(1))).collect()

        noException should be thrownBy df2.withColumn("rst_maketiles", rst_maketiles(col("raster"), lit(1000), lit(1000))).collect()

        noException should be thrownBy df2.withColumn("rst_retile", rst_retile(col("raster"), lit(1000), lit(1000))).collect()

        noException should be thrownBy
            df2.withColumn("rst_tooverlappingtiles", rst_tooverlappingtiles(col("raster"), lit(1000), lit(1000), lit(10))).collect()

        noException should be thrownBy df2.withColumn("separated", rst_separatebands(col("raster"))).collect()

    }

}
