package com.databricks.mosaic.sql.join

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.sql.MosaicFrame

object PointInPolygonJoin {

    def join(points: MosaicFrame, polygons: MosaicFrame): DataFrame = {
        val mosaicContext = MosaicContext.context
        import mosaicContext.functions._

        val chipColumnIdentifier = polygons.chipColumn.expr.sql
        val pointColumnIdentifier = points.geometryColumn.expr.sql

        points.df.join(
          polygons.df,
          points.indexColumn === polygons.indexColumn && (
            polygons.chipFlagColumn || st_contains(polygons.chipColumn, points.geometryColumn)
          )
        )

    }
}
