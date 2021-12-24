package com.databricks.mosaic.analyze

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, mean, percentile_approx}

import com.databricks.mosaic.functions.MosaicContext

object MosaicAnalyzer {

  implicit class AnalyzerImpl(df: DataFrame) {

    private def getMeanIndexArea(columnName: String, resolution: Int, mosaicContext: MosaicContext, fraction: Double): Double = {

      import mosaicContext.functions._

      val meanIndexArea = df
        .sample(fraction)
        .withColumn("centroid", st_centroid2D(col(columnName)))
        .select(
          mean(
            st_area(
              index_geometry(
                point_index(col("centroid.x"), col("centroid.y"), lit(resolution))
              )
            )
          )
        )
        .collect()
        .head
        .getDouble(0)
      meanIndexArea
    }

    def optimalResolution(columnName: String, mosaicContext: MosaicContext, fraction: Double = 0.1): ResolutionMetrics = {

      import mosaicContext.functions._

      def areaPercentile(p: Double) = percentile_approx(col("area"), lit(p), lit(10000))
      val percentiles = df
        .sample(fraction)
        .withColumn("area", st_area(col(columnName)))
        .select(
          mean("area").alias("mean"),
          areaPercentile(0.25).alias("p25"),
          areaPercentile(0.5).alias("p50"),
          areaPercentile(0.75).alias("p75")
        )
        .collect()
        .head

      val meanIndexArea = for (i <- 0 until 16)
        yield getMeanIndexArea(columnName, i, mosaicContext, fraction/10)

      ResolutionMetrics(percentiles, meanIndexArea)
    }
  }
}

