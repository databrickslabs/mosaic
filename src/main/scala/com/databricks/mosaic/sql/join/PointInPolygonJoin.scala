package com.databricks.mosaic.sql.join

import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.sql.MosaicFrame

object PointInPolygonJoin {

    def join(points: MosaicFrame, polygons: MosaicFrame): MosaicFrame = {
        val mosaicContext = MosaicContext.context
        import mosaicContext.functions._
        val (pointsProjected, polygonsProjected) =
            if (points.columns.diff(polygons.columns).isEmpty) {
                (points, polygons)
            } else {
                (points.withPrefix("points"), polygons.withPrefix("polygons"))
            }

        val joinedDf = pointsProjected.join(
          polygonsProjected,
          pointsProjected.indexColumn === polygonsProjected.indexColumn && (polygonsProjected.chipFlagColumn || st_contains(
            polygonsProjected.chipColumn,
            pointsProjected.geometryColumn
          ))
        )

        new MosaicFrame(
          joinedDf,
          pointsProjected.getGeometryColumnName,
          Some(pointsProjected.getGeometryType),
          pointsProjected.isIndexed,
          Some(pointsProjected.getIndexResolution)
        )

    }
}
