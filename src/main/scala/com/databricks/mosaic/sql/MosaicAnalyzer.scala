package com.databricks.mosaic.sql

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import com.databricks.mosaic.functions.MosaicContext

object MosaicAnalyzer {

  private var sampleFraction = 0.01

    def getOptimalResolution(df: DataFrame, columnName: String): Int = {
        val ss = SparkSession.builder().getOrCreate()
        import ss.implicits._
        val metrics = getResolutionMetrics(df, columnName, 1, 100)
            .select("resolution", "percentile_50_geometry_area")
            .as[(Int, Double)]
            .collect()
            .sortBy(_._2)
        val midInd: Int = (metrics.length - 1) / 2
        metrics(midInd)._1
    }

    def getResolutionMetrics(
        df: DataFrame,
        columnName: String,
        lowerLimit: Int = 5,
        upperLimit: Int = 500
    ): DataFrame = {
        val mosaicContext = MosaicContext.context
        import mosaicContext.functions._

        def areaPercentile(p: Double): Column = percentile_approx(col("area"), lit(p), lit(10000))

        val percentiles = df
            .sample(sampleFraction)
            .withColumn("area", st_area(col(columnName)))
            .select(
              mean("area").alias("mean"),
              areaPercentile(0.25).alias("p25"),
              areaPercentile(0.5).alias("p50"),
              areaPercentile(0.75).alias("p75")
            )
            .collect()
            .head

        val minResolution = mosaicContext.getIndexSystem.minResolution
        val maxResolution = mosaicContext.getIndexSystem.maxResolution
        val meanIndexAreas = for (i <- minResolution until maxResolution) yield (i, getMeanIndexArea(df, columnName, i))

      if (percentiles.anyNull) {
        throw new Exception(
          "Not enough geometries supplied to MosaicAnalyser to compute resolution metrics. " +
          "Try increasing the sampleFraction using the setSampleFraction method.")
      }
        val data = meanIndexAreas.map { case (resolution, indexArea) =>
            Row(
              resolution,
              indexArea,
              percentiles.getDouble(0) / indexArea,
              percentiles.getDouble(1) / indexArea,
              percentiles.getDouble(2) / indexArea,
              percentiles.getDouble(3) / indexArea
            )
        }.asJava

        val spark = SparkSession.builder().getOrCreate()
        val schema = StructType(
          Seq(
            StructField("resolution", IntegerType),
            StructField("mean_index_area", DoubleType),
            StructField("mean_geometry_area", DoubleType),
            StructField("percentile_25_geometry_area", DoubleType),
            StructField("percentile_50_geometry_area", DoubleType),
            StructField("percentile_75_geometry_area", DoubleType)
          )
        )

        spark
            .createDataFrame(
              data,
              schema = schema
            )
            .where(
              s"""
                 |($lowerLimit < mean_geometry_area and mean_geometry_area < $upperLimit) or
                 |($lowerLimit < percentile_25_geometry_area and percentile_25_geometry_area < $upperLimit) or
                 |($lowerLimit < percentile_50_geometry_area and percentile_50_geometry_area < $upperLimit) or
                 |($lowerLimit < percentile_75_geometry_area and percentile_75_geometry_area < $upperLimit)
                 |""".stripMargin
            )
    }

    private def getMeanIndexArea(df: DataFrame, columnName: String, resolution: Int): Double = {
      val ss = df.sparkSession
      import ss.implicits._
      val mosaicContext = MosaicContext.context
      import mosaicContext.functions._

      val meanIndexArea = df
          .limit(1)
          .withColumn("centroid", st_centroid2D(col(columnName)))
          .select(
              st_area(
                index_geometry(
                  point_index(
                    col("centroid").getItem("x"),
                    col("centroid").getItem("y"),
                    lit(resolution)
                  )
                )
              )
            )
            .as[Double]
            .collect
            .head
        meanIndexArea
    }

  def getSampleFraction: Double = sampleFraction

  def setSampleFraction(fraction: Double): Unit = sampleFraction = fraction

}
