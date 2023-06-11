package com.databricks.labs.mosaic.core

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.types.GeometryTypeEnum._
import com.databricks.labs.mosaic.core.types._

import scala.annotation.tailrec

/**
 * Single abstracted logic for mosaic fill via [[IndexSystem]]. [[IndexSystem]]
 * is in charge of implementing the individual steps of the logic.
 */
object Mosaic {

  /**
   * This method is used to fill a geometry with a given resolution.
   *
   * @param geometry     The geometry to fill with chips.
   * @param resolution   The resolution to fill the geometry with.
   * @param keepCoreGeom Whether or not to keep the core geometry.
   * @param indexSystem  The index system to use for filling the geometry.
   * @param geometryAPI  The geometry API to use for manipulating the geometry.
   * @return A sequence of [[MosaicChip]]s.
   */
  def getChips(
                geometry: MosaicGeometry,
                resolution: Int,
                keepCoreGeom: Boolean,
                indexSystem: IndexSystem,
                geometryAPI: GeometryAPI
              ): Seq[MosaicChip] = {
    GeometryTypeEnum.fromString(geometry.getGeometryType) match {
      case POINT => pointChip(geometry, resolution, keepCoreGeom, indexSystem)
      case MULTIPOINT => multiPointChips(geometry, resolution, keepCoreGeom, indexSystem)
      case LINESTRING => lineFill(geometry, resolution, indexSystem, geometryAPI)
      case MULTILINESTRING => lineFill(geometry, resolution, indexSystem, geometryAPI)
      case _ => mosaicFill(geometry, resolution, keepCoreGeom, indexSystem, geometryAPI)
    }
  }

  /**
   * This method is used to fill a geometry with a given resolution.
   * This method is designed to be used with a [[MosaicMultiPoint]].
   *
   * @param geometry     The geometry to fill with chips.
   * @param resolution   The resolution to fill the geometry with.
   * @param keepCoreGeom Whether or not to keep the core geometry.
   * @param indexSystem  The index system to use for filling the geometry.
   * @return A sequence of [[MosaicChip]]s.
   */
  def multiPointChips(
                       geometry: MosaicGeometry,
                       resolution: Int,
                       keepCoreGeom: Boolean,
                       indexSystem: IndexSystem
                     ): Seq[MosaicChip] = {
    val points = geometry.asInstanceOf[MosaicMultiPoint].asSeq
    points.flatMap(point => pointChip(point, resolution, keepCoreGeom, indexSystem))
  }

  /**
   * This method is used to fill a geometry with a given resolution.
   * This method is designed to be used with a [[MosaicPoint]].
   *
   * @param geometry     The geometry to fill with chips.
   * @param resolution   The resolution to fill the geometry with.
   * @param keepCoreGeom Whether or not to keep the core geometry.
   * @param indexSystem  The index system to use for filling the geometry.
   * @return A sequence of [[MosaicChip]]s.
   */
  def pointChip(
                 geometry: MosaicGeometry,
                 resolution: Int,
                 keepCoreGeom: Boolean,
                 indexSystem: IndexSystem
               ): Seq[MosaicChip] = {
    val point = geometry.asInstanceOf[MosaicPoint]
    val chipGeom = if (keepCoreGeom) point else null
    val cellId = indexSystem.pointToIndex(point.getX, point.getY, resolution)
    val chip = MosaicChip(isCore = false, Left(cellId), chipGeom)
    Seq(chip.formatCellId(indexSystem))
  }

  /**
   * This method is used to fill a geometry with a given resolution.
   * This method is designed to be used with a [[MosaicPolygon]], [[MosaicMultiPolygon]] and [[MosaicGeometryCollection]].
   *
   * @param geometry    The geometry to fill with chips.
   * @param resolution  The resolution to fill the geometry with.
   * @param indexSystem The index system to use for filling the geometry.
   * @param geometryAPI The geometry API to use for manipulating the geometry.
   * @return A sequence of [[MosaicChip]]s.
   */
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
    val borderIndices = indexSystem.polyfill(borderGeometry, resolution, Some(geometryAPI)).diff(coreIndices)

    val coreChips = indexSystem.getCoreChips(coreIndices, keepCoreGeom, geometryAPI)
    val borderChips = indexSystem.getBorderChips(geometry, borderIndices, keepCoreGeom, geometryAPI)

    coreChips ++ borderChips
  }

  /**
   * This method is used to decompose a [[MosaicLineString]] or [[MosaicMultiLineString]] into a sequence of [[MosaicChip]]s.
   *
   * @param geometry    The line to decompose.
   * @param resolution  The resolution to decompose the line with.
   * @param indexSystem The index system to use for decomposing the line.
   * @param geometryAPI The geometry API to use for manipulating the geometry.
   * @return A sequence of [[MosaicChip]]s.
   */
  def lineFill(geometry: MosaicGeometry, resolution: Int, indexSystem: IndexSystem, geometryAPI: GeometryAPI): Seq[MosaicChip] = {
    GeometryTypeEnum.fromString(geometry.getGeometryType) match {
      case LINESTRING => lineDecompose(geometry.asInstanceOf[MosaicLineString], resolution, indexSystem, geometryAPI)
      case MULTILINESTRING =>
        val multiLine = geometry.asInstanceOf[MosaicMultiLineString]
        multiLine.flatten.flatMap(line => lineDecompose(line.asInstanceOf[MosaicLineString], resolution, indexSystem, geometryAPI))
      case gt => throw new Error(s"$gt not supported for line fill/decompose operation.")
    }
  }

  /**
   * @param geometry
   * Geometry to get k ring cells for.
   * @param resolution
   * Resolution of the cells to get.
   * @param indexSystem
   * Index system to use.
   * @param geometryAPI
   * Geometry API to use.
   * @return
   * A set of k ring cells for the geometry.
   */
  //noinspection DuplicatedCode
  def geometryKRing(geometry: MosaicGeometry, resolution: Int, k: Int, indexSystem: IndexSystem, geometryAPI: GeometryAPI): Set[Long] = {
    val (coreCells, borderCells) = getCellSets(geometry, resolution, indexSystem, geometryAPI)
    val borderKRing = borderCells.flatMap(indexSystem.kRing(_, k))
    val kRing = coreCells ++ borderKRing
    kRing
  }

  /**
   * @param geometry
   * Geometry to get k loop around
   * @param resolution
   * Resolution of the cells
   * @param indexSystem
   * Index system to use
   * @param geometryAPI
   * Geometry API to use
   * @return
   * Set of cells that form a k loop around geometry
   */
  //noinspection DuplicatedCode
  def geometryKLoop(geometry: MosaicGeometry, resolution: Int, k: Int, indexSystem: IndexSystem, geometryAPI: GeometryAPI): Set[Long] = {
    val n: Int = k - 1
    // This would be much more efficient if we could use the
    // pre-computed tessellation of the geometry for repeated calls.
    val (coreCells, borderCells) = getCellSets(geometry, resolution, indexSystem, geometryAPI)

    // We use nRing as naming for kRing where k = n
    val borderNRing = borderCells.flatMap(indexSystem.kRing(_, n))
    val nRing = coreCells ++ borderNRing

    val borderKLoop = borderCells.flatMap(indexSystem.kLoop(_, k))

    val kLoop = borderKLoop -- nRing
    kLoop
  }

  /**
   * This method is used to decompose a [[MosaicLineString]] into a sequence of [[MosaicChip]]s.
   * Note that this method only works on [[MosaicLineString]]s.
   * For [[MosaicMultiLineString]]s, use [[lineFill]].
   *
   * @param line        The line to decompose.
   * @param resolution  The resolution to decompose the line with.
   * @param indexSystem The index system to use for decomposing the line.
   * @param geometryAPI The geometry API to use for manipulating the geometry.
   * @return A sequence of [[MosaicChip]]s.
   */
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
          val chip = MosaicChip(isCore = false, Left(current), lineSegment)
          val kRing = indexSystem.kRing(current, 1)

          // Ignore already processed chips and those which are already in the
          // queue to be processed
          val toQueue = kRing.filterNot((newTraversed ++ accumulator._1).contains)
          (accumulator._1 ++ toQueue, accumulator._2 ++ Seq(chip))
        } else if (newTraversed.size == 1) {
          // The line segment intersection was empty, but we only intersected the first point
          // with a single cell.
          // We need to run an intersection with a first ring because the starting point might be laying
          // exactly on the cell boundary.
          val kRing = indexSystem.kRing(current, 1)
          val toQueue = kRing.filterNot(newTraversed.contains)
          (toQueue, accumulator._2)
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

  /**
   * Returns core cells and border cells as a sets of Longs. The
   * implementation currently depends on [[getChips()]] method.
   *
   * @param geometry
   * Geometry to fill with cells.
   * @param resolution
   * Resolution of the cells.
   * @param indexSystem
   * Index system to use.
   * @param geometryAPI
   * Geometry API to use.
   * @return
   * Tuple of core cells and border cells.
   */
  private def getCellSets(
                           geometry: MosaicGeometry,
                           resolution: Int,
                           indexSystem: IndexSystem,
                           geometryAPI: GeometryAPI
                         ): (Set[Long], Set[Long]) = {
    val chips = Mosaic.getChips(geometry, resolution, keepCoreGeom = false, indexSystem, geometryAPI)
    val (coreChips, borderChips) = chips.partition(_.isCore)

    val coreCells = coreChips.map(_.cellIdAsLong(indexSystem)).toSet
    val borderCells = borderChips.map(_.cellIdAsLong(indexSystem)).toSet
    (coreCells, borderCells)
  }

}
