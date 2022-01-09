package com.databricks.mosaic.sql.join

import com.databricks.mosaic.sql.MosaicFrame
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrame, SparkSession}

object PointInPolygonJoin {

  def join(points: MosaicFrame, polygons: MosaicFrame): DataFrame = {
    val functionRegistry = SparkSession.builder().getOrCreate().sessionState.functionRegistry
    require(functionRegistry.functionExists(FunctionIdentifier("st_contains")), "Mosaic Context has not registered the functions.")

    val chipColumnIdentifier = polygons.chipColumn.expr.sql
    val pointColumnIdentifier = points.geometryColumn.expr.sql

    points.df.join(
      polygons.df,
      points.indexColumn === polygons.indexColumn && (
        polygons.chipFlagColumn || expr(s"st_contains($chipColumnIdentifier, $pointColumnIdentifier)")
        )
    )

  }
}
