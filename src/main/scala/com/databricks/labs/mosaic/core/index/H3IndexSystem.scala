package com.databricks.labs.mosaic.core.index

import scala.collection.JavaConverters._

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.POLYGON
import com.databricks.labs.mosaic.core.types.model.MosaicChip
import com.uber.h3core.H3Core
import java.{lang, util}
import org.locationtech.jts.geom.Geometry

/**
  * Implements the [[IndexSystem]] via [[H3Core]] java bindings.
  *
  * @see
  *   [[https://github.com/uber/h3-java]]
  */
object H3IndexSystem extends IndexSystem with Serializable {

    // An instance of H3Core to be used for IndexSystem implementation.
    @transient val h3: H3Core = H3Core.newInstance()

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
        val resolution: Int = res.asInstanceOf[Int]
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
      *   when performing mosaic_polyfill.
      */
    override def getBufferRadius(geometry: MosaicGeometry, resolution: Int, geometryAPI: GeometryAPI): Double = {
        val centroid = geometry.getCentroid
        val centroidIndex = h3.geoToH3(centroid.getY, centroid.getX, resolution)
        val indexGeom = indexToGeometry(centroidIndex, geometryAPI)
        val boundary = indexGeom.getShellPoints.head // first shell is always in head

        // Hexagons have only 3 diameters.
        // Computing them manually and selecting the maximum.
        // noinspection ZeroIndexToHead
        Seq(
          boundary(0).distance(boundary(3)),
          boundary(1).distance(boundary(4)),
          boundary(2).distance(boundary(5))
        ).max / 2
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
        geometryAPI.geometry(extended.map(geometryAPI.fromGeoCoord), POLYGON)
    }

    /**
      * H3 mosaic_polyfill logic is based on the centroid point of the individual index
      * geometry. Blind spots do occur near the boundary of the geometry.
      *
      * @param geometry
      *   Input geometry to be represented.
      * @param resolution
      *   A resolution of the indices.
      * @return
      *   A set of indices representing the input geometry.
      */
    override def mosaic_polyfill(geometry: MosaicGeometry, resolution: Int): util.List[java.lang.Long] = {
        if (geometry.isEmpty) Seq.empty[java.lang.Long].asJava
        else {
            val shellPoints = geometry.getShellPoints
            val holePoints = geometry.getHolePoints
            (for (i <- 0 until geometry.getNumGeometries) yield {
                val boundary = shellPoints(i).map(_.geoCoord).asJava
                val holes = holePoints(i).map(_.map(_.geoCoord).asJava).asJava

                val indices = h3.polyfill(boundary, holes, resolution)
                indices.asScala
            }).flatten.asJava
        }
    }

    /**
      * @see
      *   [[IndexSystem.getBorderChips()]]
      * @param geometry
      *   Input geometry whose border is being represented.
      * @param borderIndices
      *   Indices corresponding to the border area of the input geometry.
      * @return
      *   A border area representation via [[MosaicChip]] set.
      */
    override def getBorderChips(
        geometry: MosaicGeometry,
        borderIndices: util.List[java.lang.Long],
        geometryAPI: GeometryAPI
    ): Seq[MosaicChip] = {
        val intersections = for (index <- borderIndices.asScala) yield {
            val indexGeom = indexToGeometry(index, geometryAPI)
            val chip = MosaicChip(isCore = false, index, indexGeom)
            chip.intersection(geometry)
        }
        intersections.filterNot(_.isEmpty)
    }

    /**
      * @see
      *   [[IndexSystem.getCoreChips()]]
      * @param coreIndices
      *   Indices corresponding to the core area of the input geometry.
      * @return
      *   A core area representation via [[MosaicChip]] set.
      */
    override def getCoreChips(coreIndices: util.List[lang.Long]): Seq[MosaicChip] = {
        coreIndices.asScala.map(MosaicChip(true, _, null))
    }

    /**
      * Returns the index system ID instance that uniquely identifies an index
      * system. This instance is used to select appropriate Mosaic expressions.
      *
      * @return
      *   An instance of [[IndexSystemID]]
      */
    override def getIndexSystemID: IndexSystemID = H3

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
    override def kRing(index: Long, n: Int): util.List[lang.Long] = {
        h3.kRing(index, n)
    }

    override def minResolution: Int = 0

    override def maxResolution: Int = 16

}
