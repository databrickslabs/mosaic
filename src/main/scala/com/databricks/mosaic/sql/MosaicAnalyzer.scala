package com.databricks.mosaic.sql

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.databricks.mosaic.functions.MosaicContext


trait MosaicAnalyzer {

  var geometryColumn: Column = _
  var geometryColumnType: DataType = _
  var analyzerDataFrame: DataFrame = _

  var analyzerSampleFraction = 0.01

  val ss: SparkSession = SparkSession.builder().getOrCreate()
  import ss.implicits._

  def getOptimalResolution: Int = {
    val metrics = getResolutionMetrics(1, 100)
      .select("resolution", "percentile_50_geometry_area")
      .as[(Int, Double)]
      .collect()
      .sortBy(_._2)
    val midInd: Int = (metrics.length - 1) / 2
    metrics(midInd)._1
  }

  def getResolutionMetrics(
                            lowerLimit: Int = 5,
                            upperLimit: Int = 500
                          ): DataFrame = {
    val mosaicContext = MosaicContext.context
    import mosaicContext.functions._

    def areaPercentile(p: Double): Column = percentile_approx(col("area"), lit(p), lit(10000))

    val percentiles = analyzerDataFrame
      .sample(analyzerSampleFraction)
      .withColumn("area", st_area(geometryColumn))
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
    val meanIndexAreas = for (i <- minResolution until maxResolution) yield (i, getMeanIndexArea(i))

    if (percentiles.anyNull) {
      throw MosaicSQLExceptions.NotEnoughGeometriesException
    }
    meanIndexAreas.map { case (resolution, indexArea) =>
      List(
        resolution,
        indexArea,
        percentiles.getDouble(0) / indexArea,
        percentiles.getDouble(1) / indexArea,
        percentiles.getDouble(2) / indexArea,
        percentiles.getDouble(3) / indexArea
      )
    }.toDF("resolution", "mean_index_area", "mean_geometry_area", "percentile_25_geometry_area", "percentile_50_geometry_area", "percentile_75_geometry_area")
      .where(
        s"""
           |($lowerLimit < mean_geometry_area and mean_geometry_area < $upperLimit) or
           |($lowerLimit < percentile_25_geometry_area and percentile_25_geometry_area < $upperLimit) or
           |($lowerLimit < percentile_50_geometry_area and percentile_50_geometry_area < $upperLimit) or
           |($lowerLimit < percentile_75_geometry_area and percentile_75_geometry_area < $upperLimit)
           |""".stripMargin
      )
  }

  private def getMeanIndexArea(resolution: Int): Double = {
    val mosaicContext = MosaicContext.context
    import mosaicContext.functions._

    val meanIndexAreaDf = analyzerDataFrame
      .sample(analyzerSampleFraction)
      .withColumn("centroid", st_centroid2D(geometryColumn))
      .select(
        mean(st_area(index_geometry(point_index($"centroid.x", $"centroid.x", lit(resolution)))))
      )

    Try(meanIndexAreaDf.as[Double].collect.head) match {
      case Success(result) => result
      case Failure(_) => throw MosaicSQLExceptions.NotEnoughGeometriesException
    }
  }

}
