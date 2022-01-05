package com.databricks.mosaic.core.index

import org.apache.spark.sql.Row

case class ResolutionMetrics(
    meanRecommendedResolution: Int,
    meanAreaRatio: Double,
    p25RecommendedResolution: Int,
    p25AreaRatio: Double,
    medianRecommendedResolution: Int,
    medianAreaRatio: Double,
    p75RecommendedResolution: Int,
    p75AreaRatio: Double
)

object ResolutionMetrics {
  def apply(row: Row, indexAreas: Seq[Double]): ResolutionMetrics = {

    val geometryAreas = Seq(
      row.getDouble(row.fieldIndex("p25")),
      row.getDouble(row.fieldIndex("mean")),
      row.getDouble(row.fieldIndex("p50")),
      row.getDouble(row.fieldIndex("p75"))
    )

    //noinspection ZeroIndexToHead
    val p25Ratios = indexAreas.map(geometryAreas(0)/_).zipWithIndex.filter(i => i._1 > 10 & i._1 < 100).minBy(_._1)
    val meanRatios = indexAreas.map(geometryAreas(1)/_).zipWithIndex.filter(i => i._1 > 10 & i._1 < 100).minBy(_._1)
    val p50Ratios = indexAreas.map(geometryAreas(2)/_).zipWithIndex.filter(i => i._1 > 10 & i._1 < 100).minBy(_._1)
    val p75Ratios = indexAreas.map(geometryAreas(3)/_).zipWithIndex.filter(i => i._1 > 10 & i._1 < 100).minBy(_._1)

    ResolutionMetrics(
      meanRatios._2, meanRatios._1,
      p25Ratios._2, p25Ratios._1,
      p50Ratios._2, p50Ratios._1,
      p75Ratios._2, p75Ratios._1
    )
  }
}
