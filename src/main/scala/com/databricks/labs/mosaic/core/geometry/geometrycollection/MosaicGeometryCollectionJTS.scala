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

import java.util

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
        new InternalGeometry(GEOMETRYCOLLECTION.id, getSpatialReference, boundaries, holes)
    }

    override def getBoundary: MosaicGeometryJTS = boundary

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
        MosaicGeometryCollectionJTS
            .fromSeq(
              asSeq.map(_.mapXY(f))
            )
            .asInstanceOf[MosaicGeometryJTS]
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

    override def union(other: MosaicGeometry): MosaicGeometryJTS = {
        val union = this.collectionUnion(other)
        union.setSRID(this.getSpatialReference)
        MosaicGeometryJTS(union)
    }

    override def difference(other: MosaicGeometry): MosaicGeometryJTS = {
        val leftGeoms = this.flatten.map(_.getGeom)
        val rightGeoms = other.flatten.map(_.asInstanceOf[MosaicGeometryJTS].getGeom)
        val differences = leftGeoms.map(geom =>
            if (rightGeoms.length == 1) geom.difference(rightGeoms.head)
            else rightGeoms.map(geom.difference)
                .foldLeft(geom)((a, b) => a.intersection(b))
        )
        MosaicGeometryJTS(MosaicGeometryJTS.compactCollection(differences, getSpatialReference))
    }

    override def boundary: MosaicGeometryJTS = {
        val boundary = this.flatten.map(_.boundary.getGeom)
        MosaicGeometryJTS(MosaicGeometryJTS.compactCollection(boundary, getSpatialReference))
    }

    private def collectionUnion(other: MosaicGeometry): Geometry = {
        val leftGeoms = this.flatten.map(_.getGeom)
        val rightGeoms = other.flatten.map(_.asInstanceOf[MosaicGeometryJTS].getGeom)
        MosaicGeometryJTS.compactCollection(leftGeoms ++ rightGeoms, getSpatialReference)
    }

    override def distance(geom2: MosaicGeometry): Double = {
        val leftGeoms = this.flatten.map(_.getGeom)
        val rightGeoms = geom2.flatten.map(_.asInstanceOf[MosaicGeometryJTS].getGeom)
        leftGeoms.map(geom => rightGeoms.map(geom.distance).min).min
    }

}

object MosaicGeometryCollectionJTS extends GeometryReader {

    override def fromInternal(row: InternalRow): MosaicGeometryJTS = {
        val gf = new GeometryFactory()
        val internalGeom = InternalGeometry(row)

        val rings = internalGeom.boundaries.zip(internalGeom.holes)

        val multipolygon = rings
            .filter(_._1.nonEmpty)
            .map { case (boundaryRing, holesRings) => MosaicPolygonJTS.fromRings(boundaryRing, holesRings, internalGeom.srid) }
            .reduceOption(_ union _)

        val multilinestring = rings
            .filter(r => r._1.isEmpty && r._2.nonEmpty && r._2.head.length > 1)
            .map { case (_, holesRings) =>
                val linestring = MosaicLineStringJTS.fromSeq(holesRings.head.map(ic => {
                    val point = MosaicPointJTS.apply(ic.coords)
                    point.setSpatialReference(internalGeom.srid)
                    point
                }))
                linestring.setSpatialReference(internalGeom.srid)
                linestring.asInstanceOf[MosaicGeometryJTS]
            }
            .reduceOption(_ union _)

        val multipoint = rings
            .filter(r => r._1.isEmpty && r._2.nonEmpty && r._2.head.length == 1)
            .map { case (_, holesRings) =>
                val point = MosaicPointJTS.apply(holesRings.head.head.coords)
                point.setSpatialReference(internalGeom.srid)
                point.asInstanceOf[MosaicGeometryJTS]
            }
            .reduceOption(_ union _)

        val geometries = new util.ArrayList[Geometry]()
        multipoint.map(g => geometries.add(g.getGeom))
        multilinestring.map(g => geometries.add(g.getGeom))
        multipolygon.map(g => geometries.add(g.getGeom))

        val geometry = gf.buildGeometry(geometries)

        MosaicGeometryCollectionJTS(geometry)
    }

    override def fromSeq[T <: MosaicGeometry](
        geomSeq: Seq[T],
        geomType: GeometryTypeEnum.Value = GEOMETRYCOLLECTION
    ): MosaicGeometry = {
        MosaicGeometryCollectionJTS(
          geomSeq
              .map(_.asInstanceOf[MosaicGeometryJTS])
              .reduce((a, b) => a.union(b))
              .getGeom
        ).asInstanceOf[MosaicGeometry]

    }

    def apply(geomCollection: Geometry): MosaicGeometryCollectionJTS =
        new MosaicGeometryCollectionJTS(geomCollection.asInstanceOf[GeometryCollection])

    override def fromWKB(wkb: Array[Byte]): MosaicGeometryJTS = MosaicGeometryJTS.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometryJTS = MosaicGeometryJTS.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometryJTS = MosaicGeometryJTS.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometryJTS = MosaicGeometryJTS.fromHEX(hex)

}
