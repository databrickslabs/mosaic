package com.databricks.mosaic.core
import com.databricks.mosaic.types.model
import com.databricks.mosaic.types.model.{MosaicChip, MosaicChipESRI}
import com.databricks.mosaic.utils.GeoCoordsUtils
import com.esri.core.geometry.{SpatialReference, Point => ESRIPoint, Polygon => ESRIPolygon}
import com.esri.core.geometry.ogc.{OGCGeometry, OGCPolygon}
import com.uber.h3core.H3Core
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}

import java.{lang, util}
import scala.collection.JavaConverters._


/**
 * Implements the [[IndexSystem]] via [[H3Core]] java bindings.
 * @see [[https://github.com/uber/h3-java]]
 */
object H3IndexSystem extends IndexSystem {

  //An instance of H3Core to be used for IndexSystem implementation.
  val h3: H3Core = H3Core.newInstance()

  /**
   * H3 resolution can only be an Int value between 0 and 15.
   * @see [[IndexSystem.getResolution()]] docs.
   * @param res Any type input to be parsed into the Int representation of resolution.
   * @return Int value representing the resolution.
   */
  override def getResolution(res: Any): Int = {
    val resolution: Int = res.asInstanceOf[Int]
    if (resolution < 0 | resolution > 15)
      throw new IllegalStateException(s"H3 resolution has to be between 0 and 15; found $resolution")
    resolution
  }


  /**
   * A radius of minimal enclosing circle is always smaller than the largest side of
   * the skewed hexagon. Since H3 is generating hexagons that take into account
   * curvature of the spherical envelope a radius may be different at different localities
   * due to the skew. To address this problem a centroid hexagon is selected from
   * the geometry and the optimal radius is computed based on this hexagon.
   * @param geometry An instance of [[Geometry]] for which we are computing the optimal
   *                 buffer radius.
   * @param resolution  A resolution to be used to get the centroid index geometry.
   * @return An optimal radius to buffer the geometry in order to avoid blind spots
   *         when performing polyfill.
   */
  override def getBufferRadius(geometry: Geometry, resolution: Int): Double = {
    val centroid = geometry.getCentroid
    val centroidIndex = h3.geoToH3(centroid.getY, centroid.getX, resolution)
    val indexGeom = index2geometry(centroidIndex)
    val boundary = indexGeom.getCoordinates

    // Hexagons have only 3 diameters.
    // Computing them manually and selecting the maximum.
    Seq(
      boundary(0).distance(boundary(3)),
      boundary(1).distance(boundary(4)),
      boundary(2).distance(boundary(5))
    ).max/2
  }

  /**
   * H3 polyfill logic is based on the centroid point of the individual
   * index geometry. Blind spots do occur near the boundary of the geometry.
   * @param geometry Input geometry to be represented.
   * @param resolution A resolution of the indices.
   * @return A set of indices representing the input geometry.
   */
  override def polyfill(geometry: Geometry, resolution: Int): util.List[java.lang.Long] = {
    if (geometry.isEmpty) Seq.empty[java.lang.Long].asJava
    else {
      val boundary = GeoCoordsUtils.getBoundary(geometry)
      val holes = GeoCoordsUtils.getHoles(geometry)

      val indices = h3.polyfill(boundary, holes.asJava, resolution)
      indices
    }
  }

  override def polyfill(geometry: OGCGeometry, resolution: Int): util.List[java.lang.Long] = {
    if (geometry.isEmpty) Seq.empty[java.lang.Long].asJava
    else {
      val boundary = GeoCoordsUtils.getBoundary(geometry)
      val holes = GeoCoordsUtils.getHoles(geometry)

      val indices = h3.polyfill(boundary, holes.asJava, resolution)
      indices
    }
  }

  /**
   * @see [[IndexSystem.getBorderChips()]]
   * @param geometry Input geometry whose border is being represented.
   * @param borderIndices Indices corresponding to the border area of the
   *                      input geometry.
   *  @return A border area representation via [[MosaicChip]] set.
   */
  override def getBorderChips(geometry: Geometry, borderIndices: util.List[java.lang.Long]): Seq[MosaicChip] = {
    val intersections = for (index <- borderIndices.asScala) yield {
      val indexGeom = index2geometry(index)
      val chip = model.MosaicChip(isCore = false, index, indexGeom)
      chip.intersection(geometry)
    }
    intersections.filterNot(_.isEmpty)
  }

  /**
   * @see [[IndexSystem.getCoreChips()]]
   * @param coreIndices Indices corresponding to the core area of the
   *                    input geometry.
   *  @return A core area representation via [[MosaicChip]] set.
   */
  override def getCoreChips(coreIndices: util.List[lang.Long]): Seq[MosaicChip] = {
    coreIndices.asScala.map(MosaicChip(true,_,null))
  }

  override def getCoreChipsESRI(coreIndices: util.List[lang.Long]): Seq[MosaicChipESRI] = {
    coreIndices.asScala.map(MosaicChipESRI(true,_,null))
  }

  /**
   * Boundary that is returned by H3 isn't valid from JTS perspective since
   * it does not form a LinearRing (ie first point == last point).
   * The first point of the boundary is appended to the end of the
   * boundary to form a LinearRing.
   * @param index Id of the index whose geometry should be returned.
   * @return An instance of [[Geometry]] corresponding to index.
   */
  override def index2geometry(index: Long): Geometry = {
    val boundary = h3.h3ToGeoBoundary(index).asScala.map(cur => new Coordinate(cur.lng, cur.lat)).toList
    val extended = boundary ++ List(boundary.head)
    val geometryFactory = new GeometryFactory
    val geom = geometryFactory.createPolygon(extended.toArray)
    geom
  }

  /**
   * Returns the index system ID instance that uniquely identifies an index system.
   * This instance is used to select appropriate Mosaic expressions.
   *
   * @return An instance of [[IndexSystemID]]
   */
  override def getIndexSystemID: IndexSystemID = H3

  /**
   * Get the index ID corresponding to the provided coordinates.
   *
   * @param x          X coordinate of the point.
   * @param y          Y cooordinate of the point.
   * @param resolution Resolution of the index.
   * @return Index ID in this index system.
   */
  override def geoToIndex(x: Double, y: Double, resolution: Int): Long = {
    h3.geoToH3(x, y, resolution)
  }

  override def getBorderChips(geometry: OGCGeometry, borderIndices: util.List[lang.Long]): Seq[MosaicChipESRI] = {

    def index2geom(index: Long): OGCPolygon = {
      val spatialRef = SpatialReference.create(4326)

      val boundary = h3.h3ToGeoBoundary(index)
        .asScala //.map(cur => new Coordinate(cur.lng, cur.lat)).toList
        .map(cur => new ESRIPoint(cur.lng, cur.lat))
      val indexPolygon = new ESRIPolygon()
      indexPolygon.startPath(boundary.head)
      for (i <- 1 until boundary.length) indexPolygon.lineTo(boundary(i))
      // no need to repeat head in ESRI API
      new OGCPolygon(indexPolygon, spatialRef)
    }

    val intersections = for (index <- borderIndices.asScala) yield {
      val indexGeom = index2geom(index)
      val chip = model.MosaicChipESRI(isCore = false, index, indexGeom)
      chip.intersection(geometry)
    }
    intersections.filterNot(_.isEmpty)
  }
}
