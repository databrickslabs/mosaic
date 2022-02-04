package com.databricks.mosaic.sql

import org.apache.spark.sql._
import org.apache.spark.sql.functions.lit

import com.databricks.mosaic.core.geometry.MosaicGeometry
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.sql.MosaicJoinType.{POINT_IN_POLYGON, POLYGON_INTERSECTION}
import com.databricks.mosaic.sql.join.PointInPolygonJoin

class MosaicFrame private (
    _df: DataFrame,
    _chipColumn: Option[Column],
    _chipFlagColumn: Option[Column],
    _indexColumn: Option[Column],
    _geometryColumn: Option[Column]
) {

    def toDataset: Dataset[(MosaicGeometry, Long, Any, Boolean)] = {
        val mosaicContext = MosaicContext.context
        val geometryAPI = mosaicContext.getGeometryAPI

        val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._

        df.select(
          geometryColumn,
          indexColumn,
          chipColumn,
          chipFlagColumn
        ).map { row =>
            (
              geometryAPI.geometry(row.get(0), geometryColumn.expr.dataType),
              row.getLong(1),
              row.get(2),
              row.getBoolean(3)
            )
        }

    }

    def chipColumn: Column = _chipColumn.getOrElse(lit(null))

    def chipFlagColumn: Column = _chipFlagColumn.getOrElse(lit(null))

    def indexColumn: Column = _indexColumn.getOrElse(df.col("index"))

    def getResolutionMetrics(lowerLimit: Int = 10, upperLimit: Int = 100, fraction: Double = 0.1): DataFrame =
        MosaicAnalyzer.getResolutionMetrics(df, geometryColumn.expr.sql, lowerLimit, upperLimit, fraction)

    def df: DataFrame = _df

    def geometryColumn: Column =
        _geometryColumn.getOrElse(
          throw new IllegalStateException("Geometry column is not set!")
        )

    def setIndex(name: String): MosaicFrame = MosaicFrame(_df, _chipColumn, _chipFlagColumn, Some(_df.col(name)), _geometryColumn)

    def setGeom(name: String): MosaicFrame = MosaicFrame(_df, _chipColumn, _chipFlagColumn, _indexColumn, Some(_df.col(name)))

    def setChip(name: String, flagName: String): MosaicFrame =
        MosaicFrame(_df, Some(_df.col(name)), Some(_df.col(flagName)), _indexColumn, _geometryColumn)

    def join(other: MosaicFrame, joinType: MosaicJoinType.Value): DataFrame =
        joinType match {
            case POINT_IN_POLYGON     => pointInPolygonJoin(other)
            case POLYGON_INTERSECTION => ???
        }

    private def pointInPolygonJoin(frame: MosaicFrame): DataFrame = PointInPolygonJoin.join(this, frame)

    def prettified: DataFrame = Prettifier.prettified(df)

}

object MosaicFrame {

    def prettify(df: DataFrame): DataFrame = MosaicFrame(df).prettified

    def apply(
        df: DataFrame,
        chipColumn: Option[Column] = None,
        chipFlagColumn: Option[Column] = None,
        indexColumn: Option[Column] = None,
        geometryColumn: Option[Column] = None
    ): MosaicFrame = new MosaicFrame(df, chipColumn, chipFlagColumn, indexColumn, geometryColumn)

}
