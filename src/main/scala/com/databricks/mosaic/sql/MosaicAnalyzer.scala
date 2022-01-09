package com.databricks.mosaic.sql

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._

object MosaicAnalyzer {

  private def getMeanIndexArea(df: DataFrame, columnName: String, resolution: Int, fraction: Double): Double = {
    val functionRegistry = SparkSession.builder().getOrCreate().sessionState.functionRegistry
    require(functionRegistry.functionExists(FunctionIdentifier("st_centroid2D")), "Mosaic Context has not registered the functions.")

    val meanIndexArea = df
      .sample(fraction)
      .withColumn("centroid", expr(s"st_centroid2D($columnName)"))
      .select(
        mean(
          expr(
            s"""
               |st_area(
               |  index_geometry(
               |    point_index(centroid['x'], centroid['y'], $resolution)
               |  )
               |)
               |""".stripMargin)
        )
      )
      .collect()
      .head
      .getDouble(0)
    meanIndexArea
  }

  def getResolutionMetrics(df: DataFrame, columnName: String, lowerLimit: Int = 5, upperLimit: Int = 500, fraction: Double = 0.1): DataFrame = {
    val functionRegistry = SparkSession.builder().getOrCreate().sessionState.functionRegistry
    require(functionRegistry.functionExists(FunctionIdentifier("st_area")), "Mosaic Context has not registered the functions.")

    def areaPercentile(p: Double) = percentile_approx(col("area"), lit(p), lit(10000))

    val percentiles = df
      .sample(fraction)
      .withColumn("area", expr(s"st_area($columnName)"))
      .select(
        mean("area").alias("mean"),
        areaPercentile(0.25).alias("p25"),
        areaPercentile(0.5).alias("p50"),
        areaPercentile(0.75).alias("p75")
      )
      .collect()
      .head

    val meanIndexAreas = for (i <- 0 until 16)
      yield (i, getMeanIndexArea(df, columnName, i, fraction / 10))

    val data = meanIndexAreas.map {
      case (resolution, indexArea) => Row(
        resolution,
        indexArea,
        percentiles.getDouble(0) / indexArea,
        percentiles.getDouble(1) / indexArea,
        percentiles.getDouble(2) / indexArea,
        percentiles.getDouble(3) / indexArea
      )
    }.asJava

    val spark = SparkSession.builder().getOrCreate()
    val schema = StructType(Seq(
      StructField("resolution", IntegerType),
      StructField("mean_index_area", DoubleType),
      StructField("mean_geometry_area", DoubleType),
      StructField("percentile_25_geometry_area", DoubleType),
      StructField("percentile_50_geometry_area", DoubleType),
      StructField("percentile_75_geometry_area", DoubleType)
    ))

    spark.createDataFrame(
      data, schema = schema
    ).where(
      s"""
        (mean_geometry_area < $upperLimit or
        percentile_25_geometry_area < $upperLimit or
        percentile_50_geometry_area < $upperLimit or
        percentile_75_geometry_area < $upperLimit)
        and
        (mean_geometry_area > $lowerLimit or
        percentile_25_geometry_area > $lowerLimit or
        percentile_50_geometry_area > $lowerLimit or
        percentile_75_geometry_area > $lowerLimit)
      """
    )
  }

}
