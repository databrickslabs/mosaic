package com.databricks.labs.mosaic.core.geometry.geometrycollection

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineStringESRI
import com.databricks.labs.mosaic.core.geometry.multilinestring.MosaicMultiLineStringESRI
import com.databricks.labs.mosaic.core.geometry.multipoint.MosaicMultiPointESRI
import com.databricks.labs.mosaic.core.geometry.multipolygon.MosaicMultiPolygonESRI
import com.databricks.labs.mosaic.core.geometry.point.MosaicPointESRI
import com.databricks.labs.mosaic.core.geometry.polygon.MosaicPolygonESRI
import com.databricks.labs.mosaic.core.types.model._
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum._
import com.esri.core.geometry.Point
import com.esri.core.geometry.ogc._
import org.apache.spark.sql.catalyst.InternalRow

class MosaicGeometryCollectionESRI(geomCollection: OGCGeometryCollection)
    extends MosaicGeometryESRI(geomCollection)
      with MosaicGeometryCollection {

    override def toInternal: InternalGeometry = {
        val n = geomCollection.numGeometries()
        val geoms = for (i <- 0 until n) yield MosaicGeometryESRI(geomCollection.geometryN(i))
        val flattened = geoms
            .flatMap { g =>
                // By convention internal representation forces flattening of MULTI geometries
                GeometryTypeEnum.fromString(g.getGeometryType) match {
                    case MULTIPOLYGON    => g.flatten
                    case MULTILINESTRING => g.flatten
                    case MULTIPOINT      => g.flatten
                    case _               => Seq(g)
                }
            }
            .map {
                case p: MosaicPolygonESRI    => p.toInternal
                case l: MosaicLineStringESRI =>
                    val shell = l.toInternal.boundaries.head
                    // By convention linestrings are polygons with no shells and one hole
                    new InternalGeometry(POLYGON.id, l.getSpatialReference, Array(Array.empty), Array(Array(shell)))
                case p: MosaicPointESRI      =>
                    val shell = p.toInternal.boundaries.head
                    // By convention points are polygons with no shells and one hole with one point
                    new InternalGeometry(POLYGON.id, p.getSpatialReference, Array(Array.empty), Array(Array(shell)))
            }
        val boundaries = flattened.map(_.boundaries.head).toArray
        val holes = flattened.flatMap(_.holes).toArray
        new InternalGeometry(GEOMETRYCOLLECTION.id, getSpatialReference, boundaries, holes)
    }

    override def getBoundary: MosaicGeometryESRI = boundary

    override def getCentroid: MosaicPointESRI = {
        // Approximate centroid by getting a centroid of the convex hull of a multipoint
        // of centroids of all the geometries in the collection
        val multipoint = MosaicMultiPointESRI.fromSeq(asSeq.map(_.getCentroid))
        val centroid = multipoint.convexHull.getCentroid
        centroid
    }

    override def getArea: Double = {
        val n = geomCollection.numGeometries()
        val areas = for (i <- 0 until n) yield MosaicGeometryESRI(geomCollection.geometryN(i)).getArea
        areas.sum
    }

    override def getLength: Double = {
        val n = geomCollection.numGeometries()
        val lengths = for (i <- 0 until n) yield MosaicGeometryESRI(geomCollection.geometryN(i)).getLength
        lengths.sum
    }

    override def scale(xd: Double, yd: Double): MosaicGeometryESRI = {
        val n = geomCollection.numGeometries()
        val scaled = for (i <- 0 until n) yield MosaicGeometryESRI(geomCollection.geometryN(i)).scale(xd, yd)
        MosaicGeometryCollectionESRI.fromSeq(scaled).asInstanceOf[MosaicGeometryESRI]
    }

    override def rotate(td: Double): MosaicGeometryESRI = {
        val n = geomCollection.numGeometries()
        val rotated = for (i <- 0 until n) yield MosaicGeometryESRI(geomCollection.geometryN(i)).rotate(td)
        MosaicGeometryCollectionESRI.fromSeq(rotated).asInstanceOf[MosaicGeometryESRI]
    }

    override def translate(xd: Double, yd: Double): MosaicGeometryESRI = {
        val n = geomCollection.numGeometries()
        val translated = for (i <- 0 until n) yield MosaicGeometryESRI(geomCollection.geometryN(i)).translate(xd, yd)
        MosaicGeometryCollectionESRI.fromSeq(translated).asInstanceOf[MosaicGeometryESRI]
    }

    override def simplify(tolerance: Double): MosaicGeometryESRI = {
        val n = geomCollection.numGeometries()
        val simplified = for (i <- 0 until n) yield MosaicGeometryESRI(geomCollection.geometryN(i)).simplify(tolerance)
        MosaicGeometryCollectionESRI.fromSeq(simplified).asInstanceOf[MosaicGeometryESRI].compactGeometry
    }

    override def distance(geom2: MosaicGeometry): Double = {
        val n = geomCollection.numGeometries()
        val distances = for (i <- 0 until n) yield MosaicGeometryESRI(geomCollection.geometryN(i)).distance(geom2)
        distances.min
    }

    override def numPoints: Int = {
        getHolePoints.map(_.map(_.length).sum).sum + getShellPoints.map(_.length).sum
    }

    override def getHoles: Seq[Seq[MosaicLineStringESRI]] = {
        val n = geomCollection.numGeometries()
        val holes = for (i <- 0 until n) yield MosaicGeometryESRI(geomCollection.geometryN(i)).getHoles
        holes.flatten.map(_.map(_.asInstanceOf[MosaicLineStringESRI]))
    }

    override def getShells: Seq[MosaicLineStringESRI] = {
        val n = geomCollection.numGeometries()
        val shells = for (i <- 0 until n) yield {
            GeometryTypeEnum.fromString(geomCollection.geometryN(i).geometryType()) match {
                case GEOMETRYCOLLECTION =>
                    MosaicGeometryCollectionESRI(geomCollection.geometryN(i).asInstanceOf[OGCGeometryCollection]).getShells
                case MULTIPOLYGON       => MosaicMultiPolygonESRI(geomCollection.geometryN(i).asInstanceOf[OGCMultiPolygon]).getShells
                case POLYGON            => MosaicPolygonESRI(geomCollection.geometryN(i).asInstanceOf[OGCPolygon]).getShells
                case _                  => Seq.empty
            }
        }
        shells.flatten
    }

    override def mapXY(f: (Double, Double) => (Double, Double)): MosaicGeometryCollectionESRI = {
        MosaicGeometryCollectionESRI
            .fromSeq(
              asSeq.map(_.mapXY(f))
            )
            .asInstanceOf[MosaicGeometryCollectionESRI]
    }

    override def asSeq: Seq[MosaicGeometryESRI] =
        for (i <- 0 until geomCollection.numGeometries()) yield MosaicGeometryESRI(geomCollection.geometryN(i))

    override def flatten: Seq[MosaicGeometryESRI] = asSeq

    override def getShellPoints: Seq[Seq[MosaicPointESRI]] = getShells.map(_.asSeq)

    override def getHolePoints: Seq[Seq[Seq[MosaicPointESRI]]] = getHoles.map(_.map(_.asSeq))

    override def boundary: MosaicGeometryESRI = {
        val n = geomCollection.numGeometries()
        val boundaries = for (i <- 0 until n) yield MosaicGeometryESRI(geomCollection.geometryN(i)).boundary
        MosaicGeometryCollectionESRI.fromSeq(boundaries).asInstanceOf[MosaicGeometryESRI].compactGeometry
    }

}

object MosaicGeometryCollectionESRI extends GeometryReader {

    override def fromInternal(row: InternalRow): MosaicGeometryESRI = {
        val internalGeom = InternalGeometry(row)
        val spatialReference = MosaicGeometryESRI.getSRID(internalGeom.srid)

        val geometries = internalGeom.boundaries.zip(internalGeom.holes).map { case (boundaryRing, holesRings) =>
            if (boundaryRing.nonEmpty) {
                // POLYGON by convention, MULTIPOLYGONS are always flattened to POLYGONS in the internal representation
                val polygon = MosaicMultiPolygonESRI.createPolygon(Array(boundaryRing), Array(holesRings))
                MosaicPolygonESRI(new OGCPolygon(polygon, spatialReference))
            } else if (boundaryRing.isEmpty & holesRings.nonEmpty & holesRings.head.length > 1) {
                // LINESTRING by convention, MULTILINESTRING are always flattened to LINESTRING in the internal representation
                val polyline = MosaicMultiLineStringESRI.createPolyline(holesRings)
                val ogcLineString = new OGCLineString(polyline, 0, spatialReference)
                MosaicLineStringESRI(ogcLineString)
            } else if (holesRings.nonEmpty & holesRings.head.length == 1) {
                // POINT by convention, MULTIPOINT are always flattened to POINT in the internal representation
                val coordinates = holesRings.head.head.coords
                MosaicPointESRI(
                  new OGCPoint(new Point(coordinates(0), coordinates(1)), spatialReference)
                )
            } else {
                MosaicGeometryESRI.fromWKT("POINT EMPTY")
            }

        }

        geometries.reduce((a, b) => a.union(b))

    }

    override def fromSeq[T <: MosaicGeometry](
        geomSeq: Seq[T],
        geomType: GeometryTypeEnum.Value = GEOMETRYCOLLECTION
    ): MosaicGeometry = {
        MosaicGeometryCollectionESRI(
          geomSeq
              .map(_.asInstanceOf[MosaicGeometryESRI].getGeom)
              .reduce((a, b) => a.union(b))
        ).asInstanceOf[MosaicGeometry]

    }

    def apply(geomCollection: OGCGeometry): MosaicGeometryCollectionESRI =
        new MosaicGeometryCollectionESRI(geomCollection.asInstanceOf[OGCGeometryCollection])

    override def fromWKB(wkb: Array[Byte]): MosaicGeometryESRI = MosaicGeometryESRI.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometryESRI = MosaicGeometryESRI.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometryESRI = MosaicGeometryESRI.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometryESRI = MosaicGeometryESRI.fromHEX(hex)

}
