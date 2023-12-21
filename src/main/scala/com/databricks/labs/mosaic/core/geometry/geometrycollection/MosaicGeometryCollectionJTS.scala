package com.databricks.labs.mosaic.core.geometry.geometrycollection

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineStringJTS
import com.databricks.labs.mosaic.core.geometry.multipolygon.MosaicMultiPolygonJTS
import com.databricks.labs.mosaic.core.geometry.point.MosaicPointJTS
import com.databricks.labs.mosaic.core.geometry.polygon.MosaicPolygonJTS
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum._
import org.apache.spark.sql.catalyst.InternalRow
import org.locationtech.jts.geom._

import java.util

class MosaicGeometryCollectionJTS(geomCollection: GeometryCollection)
    extends MosaicGeometryJTS(geomCollection)
      with MosaicGeometryCollection {

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
