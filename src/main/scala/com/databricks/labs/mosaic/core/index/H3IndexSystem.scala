package com.databricks.labs.mosaic.core.index

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.types.model.Coordinates
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.{LINESTRING, POLYGON}
import com.uber.h3core.H3Core
import com.uber.h3core.util.GeoCoord
import org.apache.spark.sql.types.LongType
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Success, Try}

/**
  * Implements the [[IndexSystem]] via [[H3Core]] java bindings.
  *
  * @see
  *   [[https://github.com/uber/h3-java]]
  */
object H3IndexSystem extends IndexSystem(LongType) with Serializable {

    val name = "H3"

    // An instance of H3Core to be used for IndexSystem implementation.
    @transient private val h3: H3Core = H3Core.newInstance()

    /**
      * H3 resolution can only be an Int value between 0 and 15.
      *
      * @see
      *   [[IndexSystem.getResolution()]] docs.
      * @param res
      *   Any type input to be parsed into the Int representation of resolution.
      * @return
      *   Int value representing the resolution.
      */
    override def getResolution(res: Any): Int = {
        val resolution = (
          Try(res.asInstanceOf[Int]),
          Try(res.asInstanceOf[String]),
          Try(res.asInstanceOf[UTF8String].toString)
        ) match {
            case (Success(i), _, _) => i
            case (_, Success(s), _) => s.toInt
            case (_, _, Success(s)) => s.toInt
            case _                  => throw new IllegalArgumentException("Resolution must be an Int or String.")
        }
        if (resolution < 0 | resolution > 15) {
            throw new IllegalStateException(s"H3 resolution has to be between 0 and 15; found $resolution")
        }
        resolution
    }

    /**
      * A radius of minimal enclosing circle is always smaller than the largest
      * side of the skewed hexagon. Since H3 is generating hexagons that take
      * into account curvature of the spherical envelope a radius may be
      * different at different localities due to the skew. To address this
      * problem a centroid hexagon is selected from the geometry and the optimal
      * radius is computed based on this hexagon.
      *
      * @param geometry
      *   An instance of [[MosaicGeometry]] for which we are computing the
      *   optimal buffer radius.
      * @param resolution
      *   A resolution to be used to get the centroid index geometry.
      * @return
      *   An optimal radius to buffer the geometry in order to avoid blind spots
      *   when performing polyfill.
      */
    override def getBufferRadius(geometry: MosaicGeometry, resolution: Int, geometryAPI: GeometryAPI): Double = {
        val centroid = geometry.getCentroid
        val centroidIndex = h3.geoToH3(centroid.getY, centroid.getX, resolution)
        val indexGeom = indexToGeometry(centroidIndex, geometryAPI)
        val boundary = indexGeom.getShellPoints.head // first shell is always in head
        val indexGeomCentroid = indexGeom.getCentroid
        boundary.map(_.distance(indexGeomCentroid)).max
    }

    /**
      * Boundary that is returned by H3 isn't valid from JTS perspective since
      * it does not form a LinearRing (ie first point == last point). The first
      * point of the boundary is appended to the end of the boundary to form a
      * LinearRing.
      *
      * @param index
      *   Id of the index whose geometry should be returned.
      * @return
      *   An instance of [[Geometry]] corresponding to index.
      */
    override def indexToGeometry(index: Long, geometryAPI: GeometryAPI): MosaicGeometry = {
        val boundary = h3.h3ToGeoBoundary(index).asScala
        val extended = boundary ++ List(boundary.head)

        if (crossesNorthPole(index) || crossesSouthPole(index)) makePoleGeometry(boundary, crossesNorthPole(index), geometryAPI)
        else makeSafeGeometry(extended, geometryAPI)
    }

    /**
      * H3 polyfill logic is based on the centroid point of the individual index
      * geometry. Blind spots do occur near the boundary of the geometry.
      *
      * @param geometry
      *   Input geometry to be represented.
      * @param resolution
      *   A resolution of the indices.
      * @return
      *   A set of indices representing the input geometry.
      */
    override def polyfill(geometry: MosaicGeometry, resolution: Int, geometryAPI: Option[GeometryAPI] = None): Seq[Long] = {
        if (geometry.isEmpty) Seq.empty[Long]
        else {
            val shellPoints = geometry.getShellPoints
            val holePoints = geometry.getHolePoints
            (for (i <- 0 until geometry.getNumGeometries) yield {
                val boundary = shellPoints(i).map(_.geoCoord).map(p => new GeoCoord(p.lat, p.lng)).asJava
                val holes = holePoints(i).map(_.map(_.geoCoord).map(p => new GeoCoord(p.lat, p.lng)).asJava).asJava

                val indices = h3.polyfill(boundary, holes, resolution)
                indices.asScala.map(_.toLong)
            }).flatten
        }
    }

    /**
      * Get the index ID corresponding to the provided coordinates.
      *
      * @param lon
      *   Longitude coordinate of the point.
      * @param lat
      *   Latitude coordinate of the point.
      * @param resolution
      *   Resolution of the index.
      * @return
      *   Index ID in this index system.
      */
    override def pointToIndex(lon: Double, lat: Double, resolution: Int): Long = {
        h3.geoToH3(lat, lon, resolution)
    }

    /**
      * Get the k ring of indices around the provided index id.
      *
      * @param index
      *   Index ID to be used as a center of k ring.
      * @param n
      *   Number of k rings to be generated around the input index.
      * @return
      *   A collection of index IDs forming a k ring.
      */
    override def kRing(index: Long, n: Int): Seq[Long] = {
        h3.kRing(index, n).asScala.map(_.toLong)
    }

    /**
      * Get the k disk of indices around the provided index id.
      *
      * @param index
      *   Index ID to be used as a center of k disk.
      * @param n
      *   Distance of k disk to be generated around the input index.
      * @return
      *   A collection of index IDs forming a k disk.
      */
    override def kLoop(index: Long, n: Int): Seq[Long] = {
        // HexRing crashes in case of pentagons.
        // Ensure a KRing fallback in said case.
        require(index >= 0L)
        Try(
          h3.hexRing(index, n).asScala.map(_.toLong)
        ).getOrElse(
          h3.kRing(index, n).asScala.toSet.diff(h3.kRing(index, n - 1).asScala.toSet).toSeq.map(_.toLong)
        )
    }

    /**
      * H3 supports resolutions ranging from 0 until 15. Resolution 0 represents
      * the most coarse resolution where the surface of the earth is split into
      * 122 hexagons. Resolution 15 represents the mre fine grained resolution.
      * @see
      *   https://h3geo.org/docs/core-library/restable/
      * @return
      *   A set of supported resolutions.
      */
    override def resolutions: Set[Int] = (0 to 15).toSet

    /**
      * Get the geometry corresponding to the index with the input id.
      *
      * @param index
      *   Id of the index whose geometry should be returned.
      * @return
      *   An instance of [[MosaicGeometry]] corresponding to index.
      */
    override def indexToGeometry(index: String, geometryAPI: GeometryAPI): MosaicGeometry = {
        val boundary = h3.h3ToGeoBoundary(index).asScala
        val extended = boundary ++ List(boundary.head)

        if (crossesNorthPole(index) || crossesSouthPole(index)) makePoleGeometry(boundary, crossesNorthPole(index), geometryAPI)
        else makeSafeGeometry(extended, geometryAPI)
    }

    override def format(id: Long): String = {
        val geo = h3.h3ToGeo(id)
        h3.geoToH3Address(geo.lat, geo.lng, h3.h3GetResolution(id))
    }

    override def getResolutionStr(resolution: Int): String = resolution.toString

    override def parse(id: String): Long = {
        val geo = h3.h3ToGeo(id)
        h3.geoToH3(geo.lat, geo.lng, h3.h3GetResolution(id))
    }

    override def indexToCenter(index: Long): Coordinates = {
        val geo = h3.h3ToGeo(index)
        Coordinates(geo.lat, geo.lng)
    }

    override def indexToBoundary(index: Long): Seq[Coordinates] = {
        h3.h3ToGeoBoundary(index).asScala.map(p => Coordinates(p.lat, p.lng))
    }

    override def distance(cellId: Long, cellId2: Long): Long = Try(h3.h3Distance(cellId, cellId2)).map(_.toLong).getOrElse(0)

    // Find all cells that cross the north pole. There always is exactly one cell per resolution.
    private lazy val northPoleCells = Range.inclusive(0, 15).map(h3.geoToH3(90, 0, _))

    // Find all cells that cross the south pole. There always is exactly one cell per resolution.
    private lazy val southPoleCells = Range.inclusive(0, 15).map(h3.geoToH3(-90, 0, _))

    private def crossesNorthPole(cell_id: Long): Boolean = northPoleCells contains cell_id
    private def crossesSouthPole(cell_id: Long): Boolean = southPoleCells contains cell_id
    private def crossesNorthPole(cell_id: String): Boolean = northPoleCells contains h3.stringToH3(cell_id)
    private def crossesSouthPole(cell_id: String): Boolean = southPoleCells contains h3.stringToH3(cell_id)

    /**
      * Check if H3 cell crosses the anti-meridian. This check is not
      * generalizable for arbitrary polygons.
      * @param geometry
      *   H3 Geometry to be checked.
      * @return
      *   boolean True if the geometry crosses the anti-meridian, false
      *   otherwise.
      */
    private def crossesAntiMeridian(geometry: MosaicGeometry): Boolean = {
        val minX = geometry.minMaxCoord("X", "MIN")
        val maxX = geometry.minMaxCoord("X", "MAX")
        minX < 0 && maxX >= 0 && ((maxX - minX > 180) || !geometry.isValid)
    }

    /**
      * Shift point that falls into the western hemisphere by 360 degrees to the
      * east.
      * @param lng
      *   Longitude of the point to be shifted.
      * @param lat
      *   Latitude of the point to be shifted.
      * @return
      *   Shifted point.
      */
    private def shiftEast(lng: Double, lat: Double): (Double, Double) = {
        if (lng < 0) (lng + 360.0, lat)
        else (lng, lat)
    }

    /**
      * Shift point that lie east of the eastern hemisphere by 360 degrees to
      * the west.
      * @param lng
      *   Longitude of the point to be shifted.
      * @param lat
      *   Latitude of the point to be shifted.
      * @return
      *   Shifted point.
      */
    private def shiftWest(lng: Double, lat: Double): (Double, Double) = {
        if (lng >= 180.0) (lng - 360.0, lat)
        else (lng, lat)
    }

    /**
      * @param coordinates
      *   A collection of [[GeoCoord]]s to be used to create a
      *   [[MosaicGeometry]].
      * @param geometryAPI
      *   An instance of [[GeometryAPI]] to be used to create a
      *   [[MosaicGeometry]].
      * @return
      *   A [[MosaicGeometry]] instance. Generates a polygon using the
      *   cooridaates for the outer ring in the order they are provided. This
      *   method will not check for validity of the geometry and may return an
      *   invalid geometry.
      */
    private def makeUnsafeGeometry(coordinates: mutable.Buffer[GeoCoord], geometryAPI: GeometryAPI): MosaicGeometry = {
        geometryAPI.geometry(
          coordinates.map(p => geometryAPI.fromGeoCoord(Coordinates(p.lat, p.lng))),
          POLYGON
        )
    }

    /**
      * A BBox that covers the eastern Hemisphere
      * @param geometryAPI
      *   An instance of [[GeometryAPI]] to be used to create the geometry.
      * @return
      *   A [[MosaicGeometry]] instance.
      */
    private def makeEastBBox(geometryAPI: GeometryAPI): MosaicGeometry =
        makeUnsafeGeometry(
          mutable.Buffer(new GeoCoord(-90, 0), new GeoCoord(90, 0), new GeoCoord(90, 180), new GeoCoord(-90, 180), new GeoCoord(-90, 0)),
          geometryAPI: GeometryAPI
        )

    /**
      * A BBox that covers the western Hemisphere shifted by 360 degrees to the
      * East
      * @param geometryAPI
      *   An instance of [[GeometryAPI]] to be used to create the geometry.
      * @return
      *   A [[MosaicGeometry]] instance.
      */
    private def makeShiftedWestBBox(geometryAPI: GeometryAPI): MosaicGeometry =
        makeUnsafeGeometry(
          mutable
              .Buffer(new GeoCoord(-90, 180), new GeoCoord(90, 180), new GeoCoord(90, 360), new GeoCoord(-90, 360), new GeoCoord(-90, 180)),
          geometryAPI: GeometryAPI
        )

    /**
      * Generate a pole-safe H3 geometry. Pole geometries require two additional
      * vertices where the pole touches the anti-meridian. This method is not
      * generalizable for arbitrary polygons.
      *
      * @param coordinates
      *   A collection of [[GeoCoord]]s to be used to create a
      *   [[MosaicGeometry]].
      * @param isNorthPole
      *   Boolean indicating if the pole is the north or south pole.
      * @param geometryAPI
      *   An instance of [[GeometryAPI]] to be used to create a
      *   [[MosaicGeometry]].
      * @return
      *   A [[MosaicGeometry]] instance.
      */
    private def makePoleGeometry(coordinates: mutable.Buffer[GeoCoord], isNorthPole: Boolean, geometryAPI: GeometryAPI): MosaicGeometry = {

        val lat = if (isNorthPole) 90 else -90

        val coords = coordinates.map(geoCoord => shiftEast(geoCoord.lng, geoCoord.lat)).sortBy(_._1)
        val lineString = geometryAPI.geometry(
          coords.map(p => geometryAPI.fromGeoCoord(Coordinates(p._2, p._1))),
          LINESTRING
        )

        val westernLine = lineString.intersection(makeEastBBox(geometryAPI))
        val easternLine = lineString.intersection(makeShiftedWestBBox(geometryAPI)).mapXY(shiftWest)

        val vertices = westernLine.getShellPoints.head ++
            Seq(geometryAPI.fromGeoCoord(Coordinates(lat, 180)), geometryAPI.fromGeoCoord(Coordinates(lat, -180))) ++
            easternLine.getShellPoints.head ++ Seq(westernLine.getShellPoints.head.head)

        geometryAPI.geometry(vertices, POLYGON)

    }

    /**
      * Generate a pole-safe and antimeridian-safe H3 geometry. This method is
      * not generalizable for arbitrary polygons.
      *
      * @param coordinates
      *   A collection of [[GeoCoord]]s to be used to create a
      *   [[MosaicGeometry]].
      * @param geometryAPI
      *   An instance of [[GeometryAPI]] to be used to create a
      *   [[MosaicGeometry]].
      * @return
      *   A [[MosaicGeometry]] instance.
      */
    private def makeSafeGeometry(coordinates: mutable.Buffer[GeoCoord], geometryAPI: GeometryAPI): MosaicGeometry = {

        val unsafeGeometry = makeUnsafeGeometry(coordinates, geometryAPI)

        if (crossesAntiMeridian(unsafeGeometry)) {
            val shiftedGeometry = unsafeGeometry.mapXY(shiftEast)
            val westGeom = shiftedGeometry.intersection(makeEastBBox(geometryAPI: GeometryAPI))
            val eastGeom = shiftedGeometry.intersection(makeShiftedWestBBox(geometryAPI: GeometryAPI)).mapXY(shiftWest)
            westGeom.union(eastGeom)
        } else {
            unsafeGeometry
        }
    }

}
