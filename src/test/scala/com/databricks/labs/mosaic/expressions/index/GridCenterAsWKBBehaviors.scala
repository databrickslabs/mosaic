package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.pointDf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

trait GridCenterAsWKBBehaviors {
    this: AnyFlatSpec =>

    def GridCenterAsWKB(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val points = pointDf(spark, mc)
            .withColumn("ix", grid_pointascellid(col("geometry"), resolution))
            .withColumn("ix_centroid", grid_centeraswkb(col("ix")))
            .withColumn("centroid_wkt", st_asbinary(col("ix_centroid")))

        points.createOrReplaceTempView("centroid_indexes")

        val points2 = spark
            .sql("""| select grid_centeraswkb(ix) AS ix_centroid
                    | from centroid_indexes
                    |""".stripMargin)
            .collect()

        points.select("ix_centroid").collect() shouldBe points2
    }

}
