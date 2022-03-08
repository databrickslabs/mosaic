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
          pointsProjected.getPointIndexColumn() === polygonsProjected.getFillIndexColumn() &&
          (polygonsProjected.getChipFlagColumn() || st_contains(
            polygonsProjected.getChipColumn(),
            pointsProjected.getGeometryColumn
          ))
        )

        new MosaicFrame(joinedDf)

    }
}
