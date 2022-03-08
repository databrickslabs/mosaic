package com.databricks.mosaic.sql.join

import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.sql.MosaicFrame

object PointInPolygonJoin {

    def join(points: MosaicFrame, polygons: MosaicFrame): MosaicFrame = {
        val mosaicContext = MosaicContext.context
        import mosaicContext.functions.st_contains

        val joinedDf = points
            .join(
              polygons,
              points.getPointIndexColumn() === polygons.getFillIndexColumn() &&
              (polygons.getChipFlagColumn() ||
              st_contains(polygons.getChipColumn(), points.getGeometryColumn))
            )
        new MosaicFrame(joinedDf)
    }
}
