package com.databricks.labs.mosaic.core.geometry.geometrycollection

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineStringJTS
import com.databricks.labs.mosaic.core.geometry.multipolygon.MosaicMultiPolygonJTS
import com.databricks.labs.mosaic.core.geometry.point.MosaicPointJTS
import com.databricks.labs.mosaic.core.geometry.polygon.MosaicPolygonJTS
import com.databricks.labs.mosaic.core.types.model.{GeometryTypeEnum, InternalGeometry}
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum._
import org.apache.spark.sql.catalyst.InternalRow
import org.locationtech.jts.geom._

class MosaicGeometryCollectionJTS(geomCollection: GeometryCollection)
    extends MosaicGeometryJTS(geomCollection)
      with MosaicGeometryCollection {

    override def toInternal: InternalGeometry = {
        val n = geomCollection.getNumGeometries
        val geoms = for (i <- 0 until n) yield MosaicGeometryJTS(geomCollection.getGeometryN(i))
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
                case p: MosaicPolygonJTS    => p.toInternal
                case l: MosaicLineStringJTS =>
                    val shell = l.toInternal.boundaries.head
                    // By convention linestrings are polygons with no shells and one hole
                    new InternalGeometry(POLYGON.id, l.getSpatialReference, Array(Array.empty), Array(Array(shell)))
                case p: MosaicPointJTS      =>
                    val shell = p.toInternal.boundaries.head
                    // By convention points are polygons with no shells and one hole with one point
                    new InternalGeometry(POLYGON.id, p.getSpatialReference, Array(Array.empty), Array(Array(shell)))
            }
        val boundaries = flattened.map(_.boundaries.head).toArray
        val holes = flattened.flatMap(_.holes).toArray
        new InternalGeometry(MULTIPOLYGON.id, getSpatialReference, boundaries, holes)
    }

    override def getBoundary: MosaicGeometryJTS = {
        val geom = geomCollection.getBoundary
        geom.setSRID(geomCollection.getSRID)
        MosaicGeometryJTS(geom)
    }

    override def getShells: Seq[MosaicLineStringJTS] = {
        val n = geomCollection.getNumGeometries
        val shells = for (i <- 0 until n) yield {
            GeometryTypeEnum.fromString(geomCollection.getGeometryN(i).getGeometryType) match {
                case GEOMETRYCOLLECTION =>
                    MosaicGeometryCollectionJTS(geomCollection.getGeometryN(i).asInstanceOf[GeometryCollection]).getShells
                case MULTIPOLYGON       => MosaicMultiPolygonJTS(geomCollection.getGeometryN(i).asInstanceOf[MultiPolygon]).getShells
                case POLYGON            => MosaicPolygonJTS(geomCollection.getGeometryN(i).asInstanceOf[Polygon]).getShells
                case _                  => Seq.empty
            }
        }
        shells.flatten
    }

    override def getHoles: Seq[Seq[MosaicLineStringJTS]] = {
        val n = geomCollection.getNumGeometries
        val holes = for (i <- 0 until n) yield {
            GeometryTypeEnum.fromString(geomCollection.getGeometryN(i).getGeometryType) match {
                case GEOMETRYCOLLECTION =>
                    MosaicGeometryCollectionJTS(geomCollection.getGeometryN(i).asInstanceOf[GeometryCollection]).getHoles
                case MULTIPOLYGON       => MosaicMultiPolygonJTS(geomCollection.getGeometryN(i).asInstanceOf[MultiPolygon]).getHoles
                case POLYGON            => MosaicPolygonJTS(geomCollection.getGeometryN(i).asInstanceOf[Polygon]).getHoles
                case _                  => Seq.empty
            }
        }
        holes.flatten
    }

    override def mapXY(f: (Double, Double) => (Double, Double)): MosaicGeometryJTS = {
        MosaicGeometryCollectionJTS.fromSeq(
          asSeq.map(_.mapXY(f))
        ).asInstanceOf[MosaicGeometryJTS]
    }

    override def asSeq: Seq[MosaicGeometryJTS] =
        for (i <- 0 until geomCollection.getNumGeometries) yield {
            val geom = geomCollection.getGeometryN(i)
            geom.setSRID(geomCollection.getSRID)
            MosaicGeometryJTS(geom)
        }

    override def flatten: Seq[MosaicGeometryJTS] = asSeq

    override def getShellPoints: Seq[Seq[MosaicPointJTS]] = getShells.map(_.asSeq)

    override def getHolePoints: Seq[Seq[Seq[MosaicPointJTS]]] = getHoles.map(_.map(_.asSeq))

}

object MosaicGeometryCollectionJTS extends GeometryReader {

    override def fromInternal(row: InternalRow): MosaicGeometryJTS = {
        val gf = new GeometryFactory()
        val internalGeom = InternalGeometry(row)

        gf.createLinearRing(gf.createLineString().getCoordinates)

        val geometries = internalGeom.boundaries.zip(internalGeom.holes).map { case (boundaryRing, holesRings) =>
            if (boundaryRing.nonEmpty) {
                // POLYGON by convention, MULTIPOLYGONS are always flattened to POLYGONS in the internal representation
                MosaicPolygonJTS.fromRings(boundaryRing, holesRings, internalGeom.srid)
            } else if (boundaryRing.isEmpty & holesRings.length > 1) {
                // LINESTRING by convention, MULTILINESTRING are always flattened to LINESTRING in the internal representation
                val linestring = MosaicLineStringJTS.fromSeq(holesRings.head.map(ic => {
                    val point = MosaicPointJTS.apply(ic.coords)
                    point.setSpatialReference(internalGeom.srid)
                    point
                }))
                linestring.setSpatialReference(internalGeom.srid)
                linestring
            } else if (holesRings.length == 1) {
                // POINT by convention, MULTIPOINT are always flattened to POINT in the internal representation
                val point = MosaicPointJTS.apply(holesRings.head.head.coords)
                point.setSpatialReference(internalGeom.srid)
                point
            } else {
                MosaicGeometryJTS.fromWKT("POINT EMPTY")
            }

        }

        geometries.reduce((a, b) => a.union(b))

    }

    override def fromSeq[T <: MosaicGeometry](
        geomSeq: Seq[T],
        geomType: GeometryTypeEnum.Value = GEOMETRYCOLLECTION
    ): MosaicGeometry = {
        MosaicGeometryCollectionJTS(
          geomSeq
              .map(_.asInstanceOf[MosaicGeometryJTS].getGeom)
              .reduce((a, b) => a.union(b))
        ).asInstanceOf[MosaicGeometry]

    }

    def apply(geomCollection: Geometry): MosaicGeometryCollectionJTS =
        new MosaicGeometryCollectionJTS(geomCollection.asInstanceOf[GeometryCollection])

    override def fromWKB(wkb: Array[Byte]): MosaicGeometryJTS = MosaicGeometryJTS.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometryJTS = MosaicGeometryJTS.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometryJTS = MosaicGeometryJTS.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometryJTS = MosaicGeometryJTS.fromHEX(hex)

}
