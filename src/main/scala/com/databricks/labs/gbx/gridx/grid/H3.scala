package com.databricks.labs.gbx.gridx.grid

import com.databricks.labs.gbx.vectorx.jts.JTS
import com.uber.h3core.util.GeoCoord
import com.uber.h3core.{H3Core, LengthUnit}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.util.GeometryEditor
import org.locationtech.jts.geom.{Coordinate, Geometry, MultiPolygon, Polygon}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}

/**
  * @see
  *   [[https://github.com/uber/h3-java]]
  */
object H3 extends Serializable {

    def edgeLength(res: Int): Double = h3.edgeLength(res, LengthUnit.km)

    def crsID: Int = 4326

    val name = "H3"

    val extent: Polygon =
        JTS.polygonFromXYs(
          Array(
            (-180.0, -90.0),
            (-180.0, 90.0),
            (180.0, 90.0),
            (180.0, -90.0),
            (-180.0, -90.0)
          )
        )

    val eastBBox: Geometry =
        JTS.polygonFromXYs(
          Array(
            (-180.0, -90.0),
            (-180.0, 90.0),
            (180.0, 90.0),
            (180.0, -90.0),
            (-180.0, -90.0)
          )
        )

    val westBBox: Geometry =
        JTS.polygonFromXYs(
          Array(
            (-180.0, -90.0),
            (-180.0, 90.0),
            (0.0, 90.0),
            (0.0, -90.0),
            (-180.0, -90.0)
          )
        )

    private val shiftedWestBBox: Geometry =
        JTS.polygonFromXYs(
          Array(
            (180.0, -90.0),
            (180.0, 90.0),
            (360.0, 90.0),
            (360.0, -90.0),
            (180.0, -90.0)
          )
        )

    private val editor = new GeometryEditor()
    val shiftWest: GeometryEditor.CoordinateOperation =
        (coordinates: Array[Coordinate], _: Geometry) => {
            coordinates.map { coord =>
                if (coord.x >= 180.0) new Coordinate(coord.x - 360.0, coord.y)
                else coord
            }
        }
    val shiftEast: GeometryEditor.CoordinateOperation =
        (coordinates: Array[Coordinate], _: Geometry) => {
            coordinates.map { coord =>
                if (coord.x < 0) new Coordinate(coord.x + 360.0, coord.y)
                else coord
            }
        }

    def isCylindrical: Boolean = true

    // An instance of H3Core to be used for Grid System implementation.
    @transient private val h3: H3Core = H3Core.newInstance()

    /**
      * H3 resolution can only be an Int value between 0 and 15.
      * @param res
      *   Any type input to be parsed into the Int representation of resolution.
      * @return
      *   Int value representing the resolution.
      */
    def getResolution(res: Any): Int = {
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
      *   An instance of [[Geometry]] for which we are computing the optimal
      *   buffer radius.
      * @param resolution
      *   A resolution to be used to get the centroid cell geometry.
      * @return
      *   An optimal radius to buffer the geometry in order to avoid blind spots
      *   when performing polyfill.
      */
    def getBufferRadius(geometry: Geometry, resolution: Int): Double = {
        val centroid = geometry.getCentroid
        val (xC, yC) =
            if (centroid.getX > 180) (-180 + centroid.getX % 180, centroid.getY)
            else if (centroid.getX < -180) (180 - centroid.getX % 180, centroid.getY)
            else (centroid.getX, centroid.getY)

        val centroidCellID = h3.geoToH3(yC, xC, resolution)
        val cellGeom = cellIdToGeometry(centroidCellID)
        geometry.getGeometryType match {
            case "Polygon"      =>
                // If the geometry is a polygon, we can use its centroid to compute the radius
                cellGeom.getCoordinates.map(c => JTS.point(c).distance(centroid)).max
            case "MultiPolygon" =>
                // If the geometry is a multipolygon, we can use the centroid of the first polygon
                (for (i <- 0 until cellGeom.getNumGeometries) yield {
                    cellGeom.getGeometryN(i).getCoordinates.map(c => JTS.point(c).distance(centroid)).max
                }).max

        }
    }

    /**
      * Boundary that is returned by H3 isn't valid from JTS perspective since
      * it does not form a LinearRing (ie first point == last point). The first
      * point of the boundary is appended to the end of the boundary to form a
      * LinearRing.
      *
      * @param cellID
      *   ID of the cell whose geometry should be returned.
      * @return
      *   An instance of [[Geometry]] corresponding to cell ID.
      */
    def cellIdToGeometry(cellID: Long): Geometry = {
        val boundary = h3.h3ToGeoBoundary(cellID).asScala
        val extended = boundary ++ List(boundary.head)

        val geom =
            if (crossesNorthPole(cellID) || crossesSouthPole(cellID)) makePoleGeometry(boundary, crossesNorthPole(cellID))
            else makeSafeGeometry(extended)

        geom.setSRID(crsID)
        geom
    }

    def alignToGrid(geometry: Geometry): Geometry = {
        val extentEnvelope = geometry.getEnvelopeInternal
        val width = extentEnvelope.getMaxX - extentEnvelope.getMinX
        val central = geometry.intersection(extent)
        JTS.translate(-width, 0, extent)
        val left = JTS.translate(width, 0, geometry.intersection(JTS.translate(-width, 0, extent)))
        val right = JTS.translate(-width, 0, geometry.intersection(JTS.translate(width, 0, extent)))
        central.union(left).union(right)
    }

    /**
      * H3 polyfill logic is based on the centroid point of the individual cell
      * geometry. Blind spots do occur near the boundary of the geometry.
      *
      * @param geometry
      *   Input geometry to be represented.
      * @param resolution
      *   A resolution of the indices.
      * @return
      *   A set of indices representing the input geometry.
      */
    def polyfill(geometry: Geometry, resolution: Int): Seq[Long] = {

        def polygonToIndices(polygon: Polygon): mutable.Seq[Long] = {
            val boundary = polygon.getExteriorRing.getCoordinates.map(c => new GeoCoord(c.y, c.x)).toList.asJava
            val holes = (for (j <- 0 until polygon.getNumInteriorRing) yield {
                polygon.getInteriorRingN(j).getCoordinates.map(c => new GeoCoord(c.y, c.x)).toList.asJava
            }).toList.asJava
            h3.polyfill(boundary, holes, resolution).asScala.map(_.toLong)
        }

        def geomToIndices(geometry: Geometry): Seq[Long] = {

            (for (i <- 0 until geometry.getNumGeometries) yield {
                geometry.getGeometryN(i) match {
                    case p: Polygon       => polygonToIndices(p)
                    case mp: MultiPolygon => (for (j <- 0 until mp.getNumGeometries) yield {
                            polygonToIndices(mp.getGeometryN(j).asInstanceOf[Polygon])
                        }).flatten
                }
            }).flatten
        }

        if (geometry.isEmpty) Seq.empty[Long]
        else {
            // split the geometry across the meridian
            val westernHemi = JTS.polygonFromXYs(
              Array(
                (-180.0, -90.0),
                (-180.0, 90.0),
                (0.0, 90.0),
                (0.0, -90.0),
                (-180.0, -90.0)
              )
            )
            geomToIndices(geometry.intersection(westernHemi)) ++ geomToIndices(geometry.difference(westernHemi))
        }
    }

    /**
      * Get the cell ID corresponding to the provided coordinates.
      *
      * @param lon
      *   Longitude coordinate of the point.
      * @param lat
      *   Latitude coordinate of the point.
      * @param resolution
      *   Resolution of the grid.
      * @return
      *   Cell ID in this grid system.
      */
    def pointToCellID(lon: Double, lat: Double, resolution: Int): Long = {
        h3.geoToH3(lat, lon, resolution)
    }

    /**
      * Get the k ring of indices around the provided cell id.
      *
      * @param cellID
      *   Cell ID to be used as a center of k ring.
      * @param n
      *   Number of k rings to be generated around the input cell.
      * @return
      *   A collection of cell IDs forming a k ring.
      */
    def kRing(cellID: Long, n: Int): mutable.Seq[Long] = {
        h3.kRing(cellID, n).asScala.map(_.toLong)
    }

    /**
      * Get the k disk of indices around the provided cell id.
      *
      * @param cellID
      *   Cell ID to be used as a center of k disk.
      * @param n
      *   Distance of k disk to be generated around the input cell.
      * @return
      *   A collection of cell IDs forming a k disk.
      */
    def kLoop(cellID: Long, n: Int): mutable.Seq[Long] = {
        // HexRing crashes in case of pentagons.
        // Ensure a KRing fallback in said case.
        require(cellID >= 0L)
        Try(
          h3.hexRing(cellID, n).asScala.map(_.toLong)
        ).getOrElse(
          // TODO: this should be improveable
          // 2 runs of kring at n and n-1 seem redundant
          // just kring n and filter via distance should be better
          // h3.kRing(cellID, n).asScala.toSet.diff(h3.kRing(cellID, n - 1).asScala.toSet).map(_.toLong).toSeq
          h3.kRing(cellID, n).asScala.filter(cell => h3.h3Distance(cellID, cell) == n).map(_.toLong)
        )
    }

    /**
      * H3 supports resolutions ranging from 0 until 15. Resolution 0 represents
      * the most coarse resolution where the surface of the earth is split into
      * 122 hexagons. Resolution 15 represents the mre fine-grained resolution.
      * @see
      *   https://h3geo.org/docs/core-library/restable/
      * @return
      *   A set of supported resolutions.
      */
    def resolutions: Set[Int] = (0 to 15).toSet

    def format(id: Long): String = {
        val geo = h3.h3ToGeo(id)
        h3.geoToH3Address(geo.lat, geo.lng, h3.h3GetResolution(id))
    }

    def getResolutionStr(resolution: Int): String = resolution.toString

    def parse(id: String): Long = {
        val geo = h3.h3ToGeo(id)
        h3.geoToH3(geo.lat, geo.lng, h3.h3GetResolution(id))
    }

    def cellIdToCenter(cellID: Long): Coordinate = {
        val geo = h3.h3ToGeo(cellID)
        new Coordinate(geo.lat, geo.lng)
    }

    def cellIdToBoundary(cellID: Long): mutable.Seq[Coordinate] = {
        h3.h3ToGeoBoundary(cellID).asScala.map(p => new Coordinate(p.lat, p.lng))
    }

    def distance(cellId: Long, cellId2: Long): Long = Try(h3.h3Distance(cellId, cellId2)).map(_.toLong).getOrElse(0)

    // Find all cells that cross the North Pole. There always is exactly one cell per resolution.
    private lazy val northPoleCells = Range.inclusive(0, 15).map(h3.geoToH3(90, 0, _))

    // Find all cells that cross the South Pole. There always is exactly one cell per resolution.
    private lazy val southPoleCells = Range.inclusive(0, 15).map(h3.geoToH3(-90, 0, _))

    private def crossesNorthPole(cell_id: Long): Boolean = northPoleCells contains cell_id
    private def crossesSouthPole(cell_id: Long): Boolean = southPoleCells contains cell_id
    // noinspection ScalaUnusedSymbol
    private def crossesNorthPole(cell_id: String): Boolean = northPoleCells contains h3.stringToH3(cell_id)
    // noinspection ScalaUnusedSymbol
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
    def crossesAntiMeridian(geometry: Geometry): Boolean = {
        val envelope = geometry.getEnvelopeInternal
        val minX = envelope.getMinX
        val maxX = envelope.getMaxX
        minX < 0 && maxX >= 0 && ((maxX - minX > 180) || !geometry.isValid)
    }

    /**
      * Generate a pole-safe H3 geometry. Pole geometries require two additional
      * vertices where the pole touches the anti-meridian. This method is not
      * generalizable for arbitrary polygons.
      *
      * @param coordinates
      *   A collection of [[GeoCoord]]s to be used to create a [[Geometry]].
      * @param isNorthPole
      *   Boolean indicating if the pole is the north or South Pole.
      * @return
      *   A [[Geometry]] instance.
      */
    private def makePoleGeometry(coordinates: mutable.Buffer[GeoCoord], isNorthPole: Boolean): Geometry = {

        val lat = if (isNorthPole) 90 else -90

        val ls = JTS.lineStringXYs(coordinates.map(coord => (coord.lng, coord.lat)))
        val lineString = editor.edit(ls, shiftEast)

        val westernLine = lineString.intersection(eastBBox)
        val easternLine = lineString.intersection(shiftedWestBBox)
        val shifted = editor.edit(easternLine, shiftWest)

        JTS.polygonFromCoords(
          Array(
            westernLine.getCoordinates.head,
            new Coordinate(lat, 180),
            new Coordinate(lat, -180),
            shifted.getCoordinates.head,
            westernLine.getCoordinates.head
          )
        )

    }

    /**
      * Generate a pole-safe and antimeridian-safe H3 geometry. This method is
      * not generalizable for arbitrary polygons.
      *
      * @param coordinates
      *   A collection of [[GeoCoord]]s to be used to create a [[Geometry]].
      * @return
      *   A [[Geometry]] instance.
      */
    private def makeSafeGeometry(coordinates: mutable.Buffer[GeoCoord]): Geometry = {
        val unsafeGeometry = JTS.polygonFromXYs(coordinates.map(coord => (coord.lng, coord.lat)).toArray)
        makeSafeGeometry(unsafeGeometry)
    }

    def makeSafeGeometry(unsafeGeometry: Geometry): Geometry = {
        if (crossesAntiMeridian(unsafeGeometry)) {
            val shiftedGeometry = editor.edit(unsafeGeometry, shiftEast)
            val westGeom = shiftedGeometry.intersection(eastBBox)
            val eastGeom = shiftedGeometry.intersection(shiftedWestBBox)
            val eastGeomWithShift = editor.edit(eastGeom, shiftWest)
            westGeom.union(eastGeomWithShift)
        } else {
            unsafeGeometry
        }
    }

}
