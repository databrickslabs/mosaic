package com.databricks.labs.mosaic.sql

import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.util._

case class MosaicAnalyzer(analyzerMosaicFrame: DataFrame) {

    val defaultSampleFraction = 0.01

    def getOptimalResolution(geometryColumn: String, sampleFraction: Double): Int = {
        getOptimalResolution(geometryColumn, SampleStrategy(sampleFraction = Some(sampleFraction)))
    }

    def getOptimalResolution(geometryColumn: String): Int = getOptimalResolution(geometryColumn, SampleStrategy())

    def getOptimalResolutionStr(geometryColumn: String): String = getOptimalResolutionStr(geometryColumn, SampleStrategy())

    def getOptimalResolutionStr(geometryColumn: String, sampleStrategy: SampleStrategy): String = {
        val resolution = getOptimalResolution(geometryColumn, sampleStrategy)
        val mc = MosaicContext.context()
        mc.getIndexSystem.getResolutionStr(resolution)
    }

    def getOptimalResolution(geometryColumn: String, sampleStrategy: SampleStrategy): Int = {
        val ss = SparkSession.builder().getOrCreate()
        import ss.implicits._

        val metrics = getResolutionMetrics(geometryColumn, sampleStrategy, 1, 100)
            .select("resolution", "percentile_50_geometry_area")
            .as[(Int, Double)]
            .collect()
            .sortBy(_._2)
        val midInd: Int = (metrics.length - 1) / 2
        metrics(midInd)._1
    }

    def getResolutionMetrics(
        geometryColumn: String,
        sampleStrategy: SampleStrategy = SampleStrategy(),
        lowerLimit: Int = 5,
        upperLimit: Int = 500
    ): DataFrame = {
        val mosaicContext = MosaicContext.context()
        import mosaicContext.functions._
        val spark = SparkSession.builder().getOrCreate()

        def areaPercentile(p: Double): Column = percentile_approx(col("area"), lit(p), lit(10000))

        val percentiles = analyzerMosaicFrame
            .transform(sampleStrategy.transformer)
            .withColumn("area", st_area(col(geometryColumn)))
            .select(
              mean("area").alias("mean"),
              areaPercentile(0.25).alias("p25"),
              areaPercentile(0.5).alias("p50"),
              areaPercentile(0.75).alias("p75")
            )
            .collect()
            .head

        val meanIndexAreas = for (i <- mosaicContext.getIndexSystem.resolutions) yield (i, getMeanIndexArea(geometryColumn, sampleStrategy, i))

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

    private def getMeanIndexArea(geometryColumn: String, sampleStrategy: SampleStrategy, resolution: Int): Double = {
        val mosaicContext = MosaicContext.context()
        import mosaicContext.functions._
        val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._

        val meanIndexAreaDf = analyzerMosaicFrame
            .transform(sampleStrategy.transformer)
            .withColumn("centroid", st_centroid(col(geometryColumn)))
            .select(
              mean(
                st_area(
                  grid_boundaryaswkb(
                    grid_longlatascellid(
                      st_x(col("centroid")),
                      st_y(col("centroid")),
                      lit(resolution)
                    )
                  )
                )
              )
            )

        Try(meanIndexAreaDf.as[Double].collect.head).toOption.getOrElse(0.0)
    }

    def getOptimalResolution(geometryColumn: String, sampleRows: Int): Int = {
        getOptimalResolution(geometryColumn, SampleStrategy(sampleRows = Some(sampleRows)))
    }

}
