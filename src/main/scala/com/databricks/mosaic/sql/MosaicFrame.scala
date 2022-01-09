package com.databricks.mosaic.sql

import com.databricks.mosaic.sql.MosaicJoinType.{POINT_IN_POLYGON, POLYGON_INTERSECTION}
import com.databricks.mosaic.sql.join.PointInPolygonJoin
import org.apache.spark.sql.{Column, DataFrame}

class MosaicFrame private(
  _df: DataFrame,
  _chipColumn: Option[Column],
  _chipFlagColumn: Option[Column],
  _indexColumn: Option[Column],
  _geometryColumn: Option[Column]
) {

  def setIndex(name: String): MosaicFrame =
    MosaicFrame(_df, _chipColumn, _chipFlagColumn, Some(_df.col(name)), _geometryColumn)

  def setGeom(name: String): MosaicFrame =
    MosaicFrame(_df, _chipColumn, _chipFlagColumn, _indexColumn, Some(_df.col(name)))

  def setChip(name: String, flagName: String): MosaicFrame =
    MosaicFrame(_df, Some(_df.col(name)), Some(_df.col(flagName)), _indexColumn, _geometryColumn)

  def geometryColumn: Column = _geometryColumn.getOrElse(df.col("geometry"))

  def chipColumn: Column = _chipColumn.getOrElse(df.col("chip"))

  def chipFlagColumn: Column = _chipFlagColumn.getOrElse(df.col("chip_flag"))

  def join(other: MosaicFrame, joinType: MosaicJoinType.Value): DataFrame = joinType match {
    case POINT_IN_POLYGON => pointInPolygonJoin(other)
    case POLYGON_INTERSECTION => ???
  }

  private def pointInPolygonJoin(frame: MosaicFrame): DataFrame = PointInPolygonJoin.join(this, frame)

  def getResolutionMetrics(lowerLimit: Int, upperLimit: Int, fraction: Double = 0.1): DataFrame = MosaicAnalyzer.getResolutionMetrics(df, geometryColumn.expr.sql, lowerLimit, upperLimit, fraction)

  def indexColumn: Column = _indexColumn.getOrElse(df.col("index"))

  def df: DataFrame = _df

  def prettified: DataFrame = Prettifier.prettified(df)

}

object MosaicFrame {
  def apply(
    df: DataFrame,
    chipColumn: Option[Column] = None,
    chipFlagColumn: Option[Column] = None,
    indexColumn: Option[Column] = None,
    geometryColumn: Option[Column] = None
  ): MosaicFrame = new MosaicFrame(df, chipColumn, chipFlagColumn, indexColumn, geometryColumn)
}
