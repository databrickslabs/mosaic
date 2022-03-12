package com.databricks.mosaic.sql.join

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.sql.MosaicFrame
import com.databricks.mosaic.sql.constants.ColRoles

object PointInPolygonJoin {

    var selectedResolution: Int = _

    def join(
        points: MosaicFrame,
        polygons: MosaicFrame,
        resolution: Option[Int] = None
    ): MosaicFrame = {
        val mosaicContext = MosaicContext.context
        import mosaicContext.functions.st_contains

        selectedResolution = resolution.getOrElse(polygons.getIndexResolution)

        val indexedPoints = checkIndex(points)
        val indexedPolygons = checkIndex(polygons)

        val pointsIndexCols = indexedPoints.indexColumnMap(selectedResolution)
        val polygonsIndexCols = indexedPolygons.indexColumnMap(selectedResolution)

        // test if the indexes have been exploded
        val joinedDf =
            if (polygonsIndexCols.contains(ColRoles.CHIP_FLAG)) {
                indexedPoints.join(
                  indexedPolygons,
                  pointsIndexCols(ColRoles.INDEX) === polygonsIndexCols(ColRoles.INDEX) &&
                  (polygonsIndexCols(ColRoles.CHIP_FLAG) ||
                  st_contains(polygonsIndexCols(ColRoles.CHIP), indexedPoints.getGeometryColumn))
                )
            } else {
                val tempColName = java.util.UUID.randomUUID.toString
                val indexColumn = polygonsIndexCols(ColRoles.INDEX).getField("chips")
                indexedPoints
                    .join(
                      indexedPolygons,
                      array_contains(indexColumn.getField("h3"), pointsIndexCols(ColRoles.INDEX))
                    )
                    .withColumn(tempColName, array_position(indexColumn.getField("h3"), pointsIndexCols(ColRoles.INDEX)).cast(IntegerType))
                    .where(col(tempColName) > 0)
                    .where(
                      element_at(indexColumn.getField("is_core"), col(tempColName)) ||
                      st_contains(element_at(indexColumn.getField("wkb"), col(tempColName)), indexedPoints.getGeometryColumn)
                    )
                    .drop(tempColName)
            }

        new MosaicFrame(joinedDf)
    }

    def checkIndex(mosaicFrame: MosaicFrame): MosaicFrame =
        mosaicFrame.listIndexesForGeometry() match {
            case i if i.isEmpty                                               => applyNewIndex(mosaicFrame)
            case i if !i.exists(x => x.indexResolution == selectedResolution) => applyNewIndex(mosaicFrame)
            case _                                                            => mosaicFrame
        }

    def applyNewIndex(mosaicFrame: MosaicFrame): MosaicFrame =
        mosaicFrame
            .setIndexResolution(selectedResolution)
            .applyIndex(dropExistingIndexes = false)

}
