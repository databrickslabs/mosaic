package com.databricks.mosaic.utils

import com.uber.h3core.util.GeoCoord
import org.locationtech.jts.geom.Geometry

import java.util
import scala.collection.immutable

object GeoCoordsUtils {

  import scala.collection.JavaConverters._

  /**
   * Extracts the boundary of the geometry.
   * @param geom An instance of [[Geometry]].
   * @return A collection of [[GeoCoord]] representing the boundary.
   */
  def getBoundary(geom: Geometry): util.List[GeoCoord] = {

    def getPolygonBoundary(polygon: Geometry) =  getGeoCoords(polygon.getBoundary.getGeometryN(0))

    geom.getGeometryType match {
      case "Polygon" =>
        getPolygonBoundary(geom).asJava
      case "MultiPolygon" =>
        val n = geom.getNumGeometries
        val boundaries = for (i <- 0 until n)
          yield getPolygonBoundary(geom.getGeometryN(i))
        boundaries.reduce(_ ++ _).asJava
    }
  }

  /**
   * Extract coordinates for a geometry and returns it as a collection of [[GeoCoord]].
   * @param geom An instance of [[Geometry]].
   * @return A collection of [[GeoCoord]]
   */
  def getGeoCoords(geom: Geometry): immutable.Seq[GeoCoord] = geom.getCoordinates.toList.map(coord => new GeoCoord(coord.y, coord.x))

  /**
   * Get holes from a geometry.
   * @param geom An instance of [[Geometry]].
   * @return A collection of hole boundaries.
   */
  def getHoles(geom: Geometry): immutable.Seq[util.List[GeoCoord]] = {

    def getPolygonHoles(polygon: Geometry): immutable.Seq[util.List[GeoCoord]] = {
      val m = polygon.getBoundary.getNumGeometries
      val holes = (for(i <- 1 until m) yield polygon.getBoundary.getGeometryN(i)).map(b => getGeoCoords(b).asJava)
      holes
    }

    geom.getGeometryType match {
      case "Polygon" =>
        getPolygonHoles(geom)
      case "MultiPolygon" =>
        val n = geom.getNumGeometries
        val holeGroups = for(i <- 0 until n) yield getPolygonHoles(geom.getGeometryN(i))
        holeGroups.reduce(_ ++ _)
    }

  }
}
