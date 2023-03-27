package com.databricks.labs.mosaic.core.geometry.geometrycollection

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineStringESRI
import com.databricks.labs.mosaic.core.geometry.multilinestring.MosaicMultiLineStringESRI
import com.databricks.labs.mosaic.core.geometry.multipolygon.MosaicMultiPolygonESRI
import com.databricks.labs.mosaic.core.geometry.point.MosaicPointESRI
import com.databricks.labs.mosaic.core.geometry.polygon.MosaicPolygonESRI
import com.databricks.labs.mosaic.core.types.model._
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum._
import com.esri.core.geometry.{Point, SpatialReference}
import com.esri.core.geometry.ogc._
import org.apache.spark.sql.catalyst.InternalRow
import org.locationtech.jts.geom.GeometryFactory

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
        new InternalGeometry(MULTIPOLYGON.id, getSpatialReference, boundaries, holes)
    }

    override def getBoundary: MosaicGeometryESRI = MosaicGeometryESRI(geomCollection.boundary())

    override def getLength: Double = MosaicGeometryESRI(geomCollection.boundary()).getLength

    override def numPoints: Int = {
        getHolePoints.map(_.map(_.length).sum).sum + getShellPoints.map(_.length).sum
    }

    override def getHoles: Seq[Seq[MosaicLineStringESRI]] = {
        val n = geomCollection.numGeometries()
        val holes = for (i <- 0 until n) yield MosaicPolygonESRI(geomCollection.geometryN(i)).getHoles
        holes.flatten
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

    override def mapXY(f: (Double, Double) => (Double, Double)): MosaicMultiPolygonESRI = {
        MosaicMultiPolygonESRI.fromSeq(
          asSeq.map(_.asInstanceOf[MosaicPolygonESRI].mapXY(f).asInstanceOf[MosaicPolygonESRI])
        )
    }

    override def asSeq: Seq[MosaicGeometryESRI] =
        for (i <- 0 until geomCollection.numGeometries()) yield MosaicGeometryESRI(geomCollection.geometryN(i))

    override def flatten: Seq[MosaicGeometryESRI] = asSeq

    override def getShellPoints: Seq[Seq[MosaicPointESRI]] = getShells.map(_.asSeq)

    override def getHolePoints: Seq[Seq[Seq[MosaicPointESRI]]] = getHoles.map(_.map(_.asSeq))

}

object MosaicGeometryCollectionESRI extends GeometryReader {

    override def fromInternal(row: InternalRow): MosaicGeometryESRI = {
        val gf = new GeometryFactory()
        val internalGeom = InternalGeometry(row)

        gf.createLinearRing(gf.createLineString().getCoordinates)

        val geometries = internalGeom.boundaries.zip(internalGeom.holes).map { case (boundaryRing, holesRings) =>
            if (boundaryRing.nonEmpty) {
                // POLYGON by convention, MULTIPOLYGONS are always flattened to POLYGONS in the internal representation
                val polygon = MosaicMultiPolygonESRI.createPolygon(internalGeom.boundaries, internalGeom.holes)
                MosaicPolygonESRI(new OGCPolygon(polygon, SpatialReference.create(internalGeom.srid)))
            } else if (boundaryRing.isEmpty & holesRings.length > 1) {
                // LINESTRING by convention, MULTILINESTRING are always flattened to LINESTRING in the internal representation
                val polyline = MosaicMultiLineStringESRI.createPolyline(holesRings)
                val ogcLineString = new OGCLineString(polyline, 0, SpatialReference.create(internalGeom.srid))
                MosaicLineStringESRI(ogcLineString)
            } else if (holesRings.length == 1) {
                // POINT by convention, MULTIPOINT are always flattened to POINT in the internal representation
                val coordinates = holesRings.head.head.coords
                MosaicPointESRI(
                  new OGCPoint(new Point(coordinates(0), coordinates(1)), SpatialReference.create(defaultSpatialReferenceId))
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

    override def fromWKB(wkb: Array[Byte]): MosaicGeometryJTS = MosaicGeometryJTS.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometryJTS = MosaicGeometryJTS.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometryJTS = MosaicGeometryJTS.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometryJTS = MosaicGeometryJTS.fromHEX(hex)

}
