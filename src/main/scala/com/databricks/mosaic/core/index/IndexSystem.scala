package com.databricks.mosaic.core.index

import com.databricks.mosaic.core.geometry.MosaicGeometry
import com.databricks.mosaic.core.geometry.api.GeometryAPI
import com.databricks.mosaic.core.types.model.MosaicChip

import java.util

/**
 * Defines the API that all index systems need to respect for Mosaic to support them.
 */
trait IndexSystem extends Serializable {

  /**
   * Returns the name of the IndexSystem.
   *
   * @return IndexSystem name.
   */
  def name: String = getIndexSystemID.name

  /**
   * Returns the index system ID instance that uniquely identifies an index system.
   * This instance is used to select appropriate Mosaic expressions.
   *
   * @return An instance of [[IndexSystemID]]
   */
  def getIndexSystemID: IndexSystemID

  /**
   * Returns the resolution value based on the nullSafeEval method inputs of type Any.
   * Each Index System should ensure that only valid values of resolution are accepted.
   *
   * @param res Any type input to be parsed into the Int representation of resolution.
   * @return Int value representing the resolution.
   */
  @throws[IllegalStateException]
  def getResolution(res: Any): Int

  /**
   * Computes the radius of minimum enclosing circle of the polygon corresponding to
   * the centroid index of the provided geometry.
   *
   * @param geometry   An instance of [[MosaicGeometry]] for which we are computing the optimal
   *                   buffer radius.
   * @param resolution A resolution to be used to get the centroid index geometry.
   * @return An optimal radius to buffer the geometry in order to avoid blind spots
   *         when performing polyfill.
   */
  def getBufferRadius(geometry: MosaicGeometry, resolution: Int, geometryAPI: GeometryAPI): Double

  /**
   * Returns a set of indices that represent the input geometry. Depending on the
   * index system this set may include only indices whose centroids fall inside
   * the input geometry or any index that intersects the input geometry.
   * When extending make sure which is the guaranteed behavior of the index system.
   *
   * @param geometry   Input geometry to be represented.
   * @param resolution A resolution of the indices.
   * @return A set of indices representing the input geometry.
   */
  def polyfill(geometry: MosaicGeometry, resolution: Int): util.List[java.lang.Long]

  /**
   * Return a set of [[MosaicChip]] instances computed based on the geometry and
   * border indices. Each chip is computed via intersection between the input
   * geometry and individual index geometry.
   *
   * @param geometry      Input geometry whose border is being represented.
   * @param borderIndices Indices corresponding to the border area of the
   *                      input geometry.
   * @return A border area representation via [[MosaicChip]] set.
   */
  def getBorderChips(geometry: MosaicGeometry, borderIndices: util.List[java.lang.Long], geometryAPI: GeometryAPI): Seq[MosaicChip]

  /**
   * Return a set of [[MosaicChip]] instances computed based on the core
   * indices. Each index is converted to an instance of [[MosaicChip]].
   * These chips do not contain chip geometry since they are full contained
   * by the geometry whose core they represent.
   *
   * @param coreIndices Indices corresponding to the core area of the
   *                    input geometry.
   * @return A core area representation via [[MosaicChip]] set.
   */
  def getCoreChips(coreIndices: util.List[java.lang.Long]): Seq[MosaicChip]

  /**
   * Get the geometry corresponding to the index with the input id.
   *
   * @param index Id of the index whose geometry should be returned.
   * @return An instance of [[MosaicGeometry]] corresponding to index.
   */
  def indexToGeometry(index: Long, geometryAPI: GeometryAPI): MosaicGeometry

  /**
   * Get the index ID corresponding to the provided coordinates.
   *
   * @param x          X coordinate of the point.
   * @param y          Y coordinate of the point.
   * @param resolution Resolution of the index.
   * @return Index ID in this index system.
   */
  def pointToIndex(x: Double, y: Double, resolution: Int): Long

}
