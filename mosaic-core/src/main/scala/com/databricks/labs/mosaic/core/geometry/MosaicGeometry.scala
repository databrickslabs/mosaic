package com.databricks.labs.mosaic.core.geometry

import com.databricks.labs.mosaic.core.crs.CRSBoundsProvider
import com.databricks.labs.mosaic.core.geometry.api.GeometryWriter

import java.util.Locale

/**
 * A trait that defines supported operations on geometries.
 * Additional methods can be available in specific geometry types.
 * Every geometry framework should implement this trait.
 */
trait MosaicGeometry extends GeometryWriter with Serializable {

  /**
   * @return Returns the number of geometries in this geometry.
   */
  def getNumGeometries: Int

  /**
   * Returns the shells of this geometry as a sequence of sequences of points.
   * Each sequence of points represents a shell.
   *
   * @return Returns the shells of this geometry as a sequence of sequences of points.
   */
  def getShellPoints: Seq[Seq[MosaicPoint]]

  /**
   * Returns the holes of this geometry as a sequence of sequences of sequences of points.
   * Each sequence of points represents a hole.
   * Each sequence of holes is related to a single shell.
   * If the geometry has 3 shells, 2 holes in first shell, 0 holes in second shell and
   * 1 hole in third shell, the returned sequence will be as follows:
   * Seq( Seq( Seq( hole1, hole2 ) ), Seq( Seq() ), Seq( Seq( hole3 ) ) )
   * where hole1, hole2 and hole3 are sequences of points.
   *
   * @return Returns the holes of this geometry as a sequence of sequences of sequences of points.
   */
  def getHolePoints: Seq[Seq[Seq[MosaicPoint]]]

  /**
   * @return Returns the number of points in this geometry.
   */
  def numPoints: Int

  /**
   * Translates this geometry by given x and y distances.
   *
   * @param xd x distance
   * @param yd y distance
   * @return Returns the translated geometry.
   */
  def translate(xd: Double, yd: Double): MosaicGeometry

  /**
   * Scales this geometry by given x and y distances.
   *
   * @param xd x distance
   * @param yd y distance
   * @return Returns the scaled geometry.
   */
  def scale(xd: Double, yd: Double): MosaicGeometry

  /**
   * Rotates this geometry by given angle.
   *
   * @param td angle in degrees
   * @return Returns the rotated geometry.
   */
  def rotate(td: Double): MosaicGeometry

  /**
   * @return Returns the length of this geometry.
   */
  def getLength: Double

  /**
   * @param geom2 geometry to calculate distance to.
   * @return Returns the distance between this geometry and given geometry.
   */
  def distance(geom2: MosaicGeometry): Double

  /**
   * @param geom geometry to calculate difference to.
   * @return Returns the difference between this geometry and given geometry.
   */
  def difference(geom: MosaicGeometry): MosaicGeometry

  /**
   * @return Returns the validity of this geometry.
   */
  def isValid: Boolean

  /**
   * @return Returns the geometry type of this geometry.
   */
  def getGeometryType: String

  /**
   * @return Returns the area of this geometry.
   */
  def getArea: Double

  /**
   * @return Returns the centroid of this geometry.
   */
  def getCentroid: MosaicPoint

  /**
   * @return Returns the flag indicating if this geometry is empty.
   */
  def isEmpty: Boolean

  /**
   * @return Returns the boundary of this geometry. The boundary is also a geometry.
   */
  def getBoundary: MosaicGeometry

  /**
   * @return Returns shells of the geometry as a sequence of LineStrings. Each LineString is a shell.
   */
  def getShells: Seq[MosaicLineString]

  /**
   * Returns holes of the geometry as a sequence of sequences of LineStrings. Each LineString is a hole.
   * Each inner sequence corresponds to a single shell.
   *
   * @return Returns holes of the geometry as a sequence of sequences of LineStrings.
   */
  def getHoles: Seq[Seq[MosaicLineString]]

  /**
   * Applies given function to each point of this geometry.
   *
   * @param f function to apply
   * @return Returns the geometry with points transformed by given function.
   */
  def mapXY(f: (Double, Double) => (Double, Double)): MosaicGeometry

  /**
   * @return Returns the boundary of this geometry.
   */
  def boundary: MosaicGeometry

  /**
   * Buffer this geometry by provided distance.
   *
   * @param distance distance to buffer
   * @return Returns the buffer of this geometry.
   */
  def buffer(distance: Double): MosaicGeometry

  /**
   * Simplifies this geometry with given tolerance.
   *
   * @param tolerance tolerance to use
   * @return Returns the simplified geometry.
   */
  def simplify(tolerance: Double): MosaicGeometry

  /**
   * Computes intersection of this geometry with given geometry.
   *
   * @param other geometry to intersect with
   * @return Returns the intersection of this geometry with given geometry.
   */
  def intersection(other: MosaicGeometry): MosaicGeometry

  /**
   * Computes the intersects flag of this geometry with given geometry.
   *
   * @param other geometry to union with
   * @return Returns the intersects flag of this geometry with given geometry.
   */
  def intersects(other: MosaicGeometry): Boolean

  /**
   * @return Returns the envelope of this geometry.
   */
  def envelope: MosaicGeometry

  /**
   * Computes union of this geometry with given geometry.
   *
   * @param other geometry to union with
   * @return Returns the union of this geometry with given geometry.
   */
  def union(other: MosaicGeometry): MosaicGeometry

  /**
   * @return Returns the unary union of this geometry.
   */
  def unaryUnion: MosaicGeometry

  /**
   * Computes the contains flag of this geometry with given geometry.
   *
   * @param other geometry to union with
   * @return Returns the contains flag of this geometry with given geometry.
   */
  def contains(other: MosaicGeometry): Boolean

  /**
   * Flattens this geometry into a collection of geometries.
   *
   * @return Returns the flattened geometry sequence.
   */
  def flatten: Seq[MosaicGeometry]

  /**
   * @return Returns the equality flag of this geometry with given geometry.
   */
  def equals(other: MosaicGeometry): Boolean

  /**
   * @return Returns the equality flag of this geometry with given geometry.
   */
  def equals(other: java.lang.Object): Boolean

  /**
   * @return Returns the equality flag of this geometry with given geometry.
   */
  def equalsTopo(other: MosaicGeometry): Boolean

  /**
   * @return Returns the hash code of this geometry.
   */
  def hashCode: Int

  /**
   * @return Returns the convex hull of this geometry.
   */
  def convexHull: MosaicGeometry

  /**
   * Computes MIN or MAX coordinate of this geometry.
   * The coordinate is selected by given dimension.
   * The function is selected by given func.
   *
   * @param dimension dimension to select coordinate from
   *                  (X, Y or Z)
   *                  (case insensitive)
   * @param func      function to select coordinate by
   *                  (MIN or MAX)
   * @return Returns the MIN or MAX coordinate of this geometry.
   */
  def minMaxCoord(dimension: String, func: String): Double = {
    val coordArray = this.getShellPoints.map(shell => {
      val unitArray = dimension.toUpperCase(Locale.ROOT) match {
        case "X" => shell.map(_.getX)
        case "Y" => shell.map(_.getY)
        case "Z" => shell.map(_.getZ)
      }
      func.toUpperCase(Locale.ROOT) match {
        case "MIN" => unitArray.min
        case "MAX" => unitArray.max
      }
    })
    func.toUpperCase(Locale.ROOT) match {
      case "MIN" => coordArray.min
      case "MAX" => coordArray.max
    }
  }

  /**
   * Transforms this geometry to given CRS.
   *
   * @param sridTo target CRS
   * @return Returns the transformed geometry.
   */
  def transformCRSXY(sridTo: Int): MosaicGeometry

  /**
   * Transforms this geometry from given CRS to given CRS.
   *
   * @param sridTo   target CRS
   * @param sridFrom source CRS
   * @return Returns the transformed geometry.
   */
  def transformCRSXY(sridTo: Int, sridFrom: Int): MosaicGeometry = {
    transformCRSXY(sridTo, Some(sridFrom))
  }

  /**
   * Transforms this geometry from given CRS to given CRS.
   *
   * @param sridTo   target CRS
   * @param sridFrom source CRS
   * @return Returns the transformed geometry.
   */
  def transformCRSXY(sridTo: Int, sridFrom: Option[Int]): MosaicGeometry

  /**
   * @return Returns the spatial reference of this geometry.
   */
  def getSpatialReference: Int

  /**
   * Sets the spatial reference of this geometry.
   *
   * @param srid spatial reference to set
   */
  def setSpatialReference(srid: Int): Unit

  /**
   * Checks if this geometry has all valid coordinates in given CRS.
   *
   * @param crsBoundsProvider CRS bounds provider
   *                          (to get bounds of given CRS)
   * @param crsCode           CRS code to check coordinates in (e.g. EPSG:4326)
   * @param which             which bounds to check (bounds or reprojected_bounds)
   * @return Returns the geometry type of this geometry.
   */
  def hasValidCoords(crsBoundsProvider: CRSBoundsProvider, crsCode: String, which: String): Boolean = {
    val crsCodeIn = crsCode.split(":")
    val crsBounds = which.toLowerCase(Locale.ROOT) match {
      case "bounds" => crsBoundsProvider.bounds(crsCodeIn(0), crsCodeIn(1).toInt)
      case "reprojected_bounds" => crsBoundsProvider.reprojectedBounds(crsCodeIn(0), crsCodeIn(1).toInt)
      case _ => throw new Error("Only boundary and reprojected_boundary supported for which argument.")
    }
    (Seq(getShellPoints) ++ getHolePoints).flatten.flatten.forall(point =>
      crsBounds.getLowerX <= point.getX && point.getX <= crsBounds.getUpperX &&
        crsBounds.getLowerY <= point.getY && point.getY <= crsBounds.getUpperY
    )
  }

}
