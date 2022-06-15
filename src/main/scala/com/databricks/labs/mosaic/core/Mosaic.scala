package com.databricks.labs.mosaic.core

import scala.annotation.tailrec

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineString
import com.databricks.labs.mosaic.core.geometry.multilinestring.MosaicMultiLineString
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.types.model.{GeometryTypeEnum, MosaicChip}
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.{LINESTRING, MULTILINESTRING}

/**
  * Single abstracted logic for mosaic fill via [[IndexSystem]]. [[IndexSystem]]
  * is in charge of implementing the individual steps of the logic.
  */
object Mosaic {

    def mosaicFill(
        geometry: MosaicGeometry,
        resolution: Int,
        keepCoreGeom: Boolean,
        indexSystem: IndexSystem,
        geometryAPI: GeometryAPI
    ): Seq[MosaicChip] = {

        val radius = indexSystem.getBufferRadius(geometry, resolution, geometryAPI)

        // do not modify the radius
        val carvedGeometry = geometry.buffer(-radius)
        // add 1% to the radius to ensure union of carved and border geometries does not have holes inside the original geometry areas
        val borderGeometry =
            if (carvedGeometry.isEmpty) {
                geometry.buffer(radius * 1.01).simplify(0.01 * radius)
            } else {
                geometry.boundary.buffer(radius * 1.01).simplify(0.01 * radius)
            }

        val coreIndices = indexSystem.polyfill(carvedGeometry, resolution, Some(geometryAPI))
        val borderIndices = indexSystem.polyfill(borderGeometry, resolution, Some(geometryAPI))

        val coreChips = indexSystem.getCoreChips(coreIndices, keepCoreGeom, geometryAPI)
        val borderChips = indexSystem.getBorderChips(geometry, borderIndices, keepCoreGeom, geometryAPI)

        coreChips ++ borderChips
    }

    def lineFill(geometry: MosaicGeometry, resolution: Int, indexSystem: IndexSystem, geometryAPI: GeometryAPI): Seq[MosaicChip] = {
        GeometryTypeEnum.fromString(geometry.getGeometryType) match {
            case LINESTRING      => lineDecompose(geometry.asInstanceOf[MosaicLineString], resolution, indexSystem, geometryAPI)
            case MULTILINESTRING =>
                val multiLine = geometry.asInstanceOf[MosaicMultiLineString]
                multiLine.flatten.flatMap(line => lineDecompose(line.asInstanceOf[MosaicLineString], resolution, indexSystem, geometryAPI))
        }
    }

    private def lineDecompose(
        line: MosaicLineString,
        resolution: Int,
        indexSystem: IndexSystem,
        geometryAPI: GeometryAPI
    ): Seq[MosaicChip] = {
        val start = line.getShells.head.asSeq.head
        val startIndex = indexSystem.pointToIndex(start.getX, start.getY, resolution)

        @tailrec
        def traverseLine(
            line: MosaicLineString,
            queue: Seq[Long],
            traversed: Set[Long],
            chips: Seq[MosaicChip]
        ): Seq[MosaicChip] = {
            val newTraversed = traversed ++ queue
            val (newQueue, newChips) = queue.foldLeft(
              (Seq.empty[Long], chips)
            )((accumulator: (Seq[Long], Seq[MosaicChip]), current: Long) => {
                val indexGeom = indexSystem.indexToGeometry(current, geometryAPI)
                val lineSegment = line.intersection(indexGeom)
                if (!lineSegment.isEmpty) {
                    val chip = MosaicChip(isCore = false, current, lineSegment)
                    val kRing = indexSystem.kRing(current, 1)
                    val toQueue = kRing.filterNot(newTraversed.contains)
                    (toQueue, accumulator._2 ++ Seq(chip))
                } else {
                    accumulator
                }
            })
            if (newQueue.isEmpty) {
                newChips
            } else {
                traverseLine(line, newQueue, newTraversed, newChips)
            }
        }

        val result = traverseLine(line, Seq(startIndex), Set.empty[Long], Seq.empty[MosaicChip])
        result
    }

}
