package com.databricks.labs.mosaic.sql.join

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.sql.MosaicFrame
import com.databricks.labs.mosaic.sql.constants.{ColMetaTags, ColRoles}

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object PointInPolygonJoin {

    var selectedResolution: Int = _

    def join(
        points: MosaicFrame,
        polygons: MosaicFrame,
        resolution: Option[Int] = None
    ): MosaicFrame = {
        selectedResolution = resolution.getOrElse(polygons.getIndexResolution)

        val indexedPoints = checkIndex(points)
        val indexedPolygons = checkIndex(polygons)

        val pointsIndexCol = indexedPoints.indexColumnMap(selectedResolution)(ColRoles.INDEX)
        val polygonsIndexCol = indexedPolygons.indexColumnMap(selectedResolution)(ColRoles.INDEX)

        // test if the indexes have been exploded
        val joinedDf =
            if (indexedPolygons.schema(polygonsIndexCol.toString).metadata.getBoolean(ColMetaTags.EXPLODED_POLYFILL)) {
                joinExplodedRows(indexedPoints, indexedPolygons, pointsIndexCol, polygonsIndexCol)
            } else {
                joinArrayRows(indexedPoints, indexedPolygons, pointsIndexCol, polygonsIndexCol)
            }

        new MosaicFrame(joinedDf)
    }

    private def joinArrayRows(
        indexedPoints: MosaicFrame,
        indexedPolygons: MosaicFrame,
        pointsIndexCol: Column,
        polygonsIndexCol: Column
    ) = {

        val mosaicContext = MosaicContext.context
        import mosaicContext.functions.st_contains

        val polygonIndexChips = polygonsIndexCol.getField("chips")
        val tempColName = java.util.UUID.randomUUID.toString
        indexedPoints
            .join(
              indexedPolygons,
              array_contains(polygonIndexChips.getField("index_id"), pointsIndexCol)
            )
            .withColumn(
              tempColName,
              array_position(polygonIndexChips.getField("index_id"), pointsIndexCol).cast(IntegerType)
            )
            .where(col(tempColName) > 0)
            .where(
              element_at(polygonIndexChips.getField("is_core"), col(tempColName)) ||
              st_contains(element_at(polygonIndexChips.getField("wkb"), col(tempColName)), indexedPoints.getGeometryColumn)
            )
            .drop(tempColName)
    }

    private def joinExplodedRows(
        indexedPoints: MosaicFrame,
        indexedPolygons: MosaicFrame,
        pointsIndexCol: Column,
        polygonsIndexCol: Column
    ) = {

        val mosaicContext = MosaicContext.context
        import mosaicContext.functions.st_contains

        indexedPoints.join(
          indexedPolygons,
          pointsIndexCol === polygonsIndexCol.getField("index_id") &&
          (polygonsIndexCol.getField("is_core") ||
          st_contains(polygonsIndexCol.getField("wkb"), indexedPoints.getGeometryColumn))
        )
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
