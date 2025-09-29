package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.functions
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._

class RST_ConstructorsEvalTest extends PlanTest with SilentSparkSession {

    test("RST_ConstructorsEvalTest should evaluate expressions on raster columns") {
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
            .withColumn("rst_1", rst_fromfile(col("path"), lit("GTiff")))
            .withColumn("rst_2", rst_fromfile(col("path"), lit("GTiff")))
            .withColumn("rst_3", rst_frombands(array(col("rst_1"), col("rst_2"))))

        noException should be thrownBy {
            df.collect()
        }

        val df2 = spark.read
            .format("binaryFile")
            .load(tifPath)
            .withColumn("rst_1", rst_fromcontent(col("content"), lit("GTiff")))
            .withColumn("rst_2", rst_fromcontent(col("content"), lit("GTiff")))
            .withColumn("rst_3", rst_frombands(array(col("rst_1"), col("rst_2"))))

        noException should be thrownBy {
            df2.collect()
        }

    }

}
