package com.databricks.mosaic.sql

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import com.databricks.mosaic.functions.MosaicContext

class MosaicAnalyzer(analyzerMosaicFrame: MosaicFrame) {

    val spark: SparkSession = analyzerMosaicFrame.sparkSession
    import spark.implicits._
    val mosaicContext: MosaicContext = MosaicContext.context
    import mosaicContext.functions._

    val defaultSampleFraction = 0.01

    def getOptimalResolution(sampleFraction: Double): Int = {
        getOptimalResolution(SampleStrategy(sampleFraction = Some(sampleFraction)))
    }

    private def getOptimalResolution(sampleStrategy: SampleStrategy): Int = {

        val metrics = getResolutionMetrics(sampleStrategy, 1, 100)
            .select("resolution", "percentile_50_geometry_area")
            .as[(Int, Double)]
            .collect()
            .sortBy(_._2)
        val midInd: Int = (metrics.length - 1) / 2
        metrics(midInd)._1
    }

    def getResolutionMetrics(sampleStrategy: SampleStrategy, lowerLimit: Int = 5, upperLimit: Int = 500): DataFrame = {
        def areaPercentile(p: Double): Column = percentile_approx(col("area"), lit(p), lit(10000))

        val percentiles = analyzerMosaicFrame
            .transform(sampleStrategy.transformer)
            .withColumn("area", st_area(analyzerMosaicFrame.getGeometryColumn))
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
        val meanIndexAreas = for (i <- minResolution until maxResolution) yield (i, getMeanIndexArea(sampleStrategy, i))

        if (percentiles.anyNull) {
            throw MosaicSQLExceptions.NotEnoughGeometriesException
        }

        val indexAreaRows = meanIndexAreas
            .map({ case (resolution, indexArea) =>
                Row(
                  resolution,
                  indexArea,
                  percentiles.getDouble(0) / indexArea,
                  percentiles.getDouble(1) / indexArea,
                  percentiles.getDouble(2) / indexArea,
                  percentiles.getDouble(3) / indexArea
                )
            })
            .toList

        val indexAreaSchema = StructType(
          List(
            StructField("resolution", IntegerType, nullable = false),
            StructField("mean_index_area", DoubleType, nullable = false),
            StructField("mean_geometry_area", DoubleType, nullable = false),
            StructField("percentile_25_geometry_area", DoubleType, nullable = false),
            StructField("percentile_50_geometry_area", DoubleType, nullable = false),
            StructField("percentile_75_geometry_area", DoubleType, nullable = false)
          )
        )

        spark
            .createDataFrame(spark.sparkContext.parallelize(indexAreaRows), indexAreaSchema)
            .where(
              s"""
                 |($lowerLimit < mean_geometry_area and mean_geometry_area < $upperLimit) or
                 |($lowerLimit < percentile_25_geometry_area and percentile_25_geometry_area < $upperLimit) or
                 |($lowerLimit < percentile_50_geometry_area and percentile_50_geometry_area < $upperLimit) or
                 |($lowerLimit < percentile_75_geometry_area and percentile_75_geometry_area < $upperLimit)
                 |""".stripMargin
            )
    }

    private def getMeanIndexArea(sampleStrategy: SampleStrategy, resolution: Int): Double = {
        val mosaicContext = MosaicContext.context
        import mosaicContext.functions._
        import spark.implicits._

        val meanIndexAreaDf = analyzerMosaicFrame
            .transform(sampleStrategy.transformer)
            .withColumn("centroid", st_centroid2D(analyzerMosaicFrame.getGeometryColumn))
            .select(
              mean(st_area(index_geometry(point_index($"centroid.x", $"centroid.x", lit(resolution)))))
            )

        Try(meanIndexAreaDf.as[Double].collect.head) match {
            case Success(result) => result
            case Failure(_)      => throw MosaicSQLExceptions.NotEnoughGeometriesException
        }
    }

    def getOptimalResolution(sampleRows: Int): Int = {
        getOptimalResolution(SampleStrategy(sampleRows = Some(sampleRows)))
    }

}

case class SampleStrategy(sampleFraction: Option[Double] = None, sampleRows: Option[Int] = None) {
    def transformer(df: DataFrame): DataFrame = {
        (sampleFraction, sampleRows) match {
            case (Some(d), None)    => df.sample(d)
            case (None, Some(l))    => df.limit(l)
            case (Some(d), Some(l)) => df.limit(l)
            case _                  => df
        }
    }
}
