package com.databricks.labs.mosaic.core.geometry.polygon

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.linestring.{MosaicLineString, MosaicLineStringJTS}
import com.databricks.labs.mosaic.core.geometry.point.{MosaicPoint, MosaicPointJTS}
import com.databricks.labs.mosaic.core.types.model._
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum._
import org.apache.spark.sql.catalyst.InternalRow
import org.locationtech.jts.geom._

class MosaicPolygonJTS(polygon: Polygon) extends MosaicGeometryJTS(polygon) with MosaicPolygon {

    override def toInternal: InternalGeometry = {
        val boundary = polygon.getBoundary
        val shell = boundary.getGeometryN(0).getCoordinates.map(InternalCoord(_))
        val holes = for (i <- 1 until boundary.getNumGeometries) yield boundary.getGeometryN(i).getCoordinates.map(InternalCoord(_))
        new InternalGeometry(POLYGON.id, getSpatialReference, Array(shell), Array(holes.toArray))
    }

    override def getBoundary: MosaicGeometry = {
        val boundaryRing = polygon.getBoundary
        boundaryRing.setSRID(polygon.getSRID)
        MosaicGeometryJTS(boundaryRing)
    }

    override def mapXY(f: (Double, Double) => (Double, Double)): MosaicGeometry = {
        val shellTransformed = getShells.head.asInstanceOf[MosaicLineStringJTS].mapXY(f).asInstanceOf[MosaicLineStringJTS]
        val holesTransformed = getHoles.head.map(_.asInstanceOf[MosaicLineStringJTS].mapXY(f).asInstanceOf[MosaicLineStringJTS])
        val newGeom = MosaicPolygonJTS.fromSeq(Seq(shellTransformed) ++ holesTransformed)
        newGeom.setSpatialReference(getSpatialReference)
        newGeom
    }

    override def getShells: Seq[MosaicLineString] = {
        val ring = polygon.getExteriorRing
        ring.setSRID(polygon.getSRID)
        Seq(MosaicLineStringJTS(ring))
    }

    override def getHoles: Seq[Seq[MosaicLineString]] =
        Seq(for (i <- 0 until polygon.getNumInteriorRing) yield {
            val ring = polygon.getInteriorRingN(i)
            ring.setSRID(polygon.getSRID)
            MosaicLineStringJTS(ring)
        })

    override def asSeq: Seq[MosaicLineString] = getShells ++ getHoles.flatten

}

object MosaicPolygonJTS extends GeometryReader {

    def getPoints(linearRing: LinearRing): Seq[MosaicPoint] = {
        linearRing.getCoordinates.map(MosaicPointJTS(_, linearRing.getSRID))
    }

    override def fromInternal(row: InternalRow): MosaicGeometry = {
        val gf = new GeometryFactory()
        val internalGeom = InternalGeometry(row)
        val shell = gf.createLinearRing(internalGeom.boundaries.head.map(_.toCoordinate))
        val holes = internalGeom.holes.head.map(ring => ring.map(_.toCoordinate)).map(gf.createLinearRing)
        val geometry = gf.createPolygon(shell, holes)
        geometry.setSRID(internalGeom.srid)
        MosaicGeometryJTS(geometry)
    }

    override def fromSeq[T <: MosaicGeometry](geomSeq: Seq[T], geomType: GeometryTypeEnum.Value = POLYGON): MosaicPolygonJTS = {
        val gf = new GeometryFactory()
        if (geomSeq.isEmpty) {
            // For empty sequence return an empty geometry with default Spatial Reference
            return MosaicPolygonJTS(gf.createPolygon())
        }
        val spatialReference = geomSeq.head.getSpatialReference
        val newGeom = GeometryTypeEnum.fromString(geomSeq.head.getGeometryType) match {
            case POINT                         =>
                val extractedPoints = geomSeq.map(_.asInstanceOf[MosaicPointJTS])
                val exteriorRing = extractedPoints.map(_.coord).toArray ++ Array(extractedPoints.head.coord)
                gf.createPolygon(exteriorRing)
            case LINESTRING                    =>
                val extractedLines = geomSeq.map(_.asInstanceOf[MosaicLineStringJTS])
                val exteriorRing =
                    gf.createLinearRing(extractedLines.head.asSeq.map(_.coord).toArray ++ Array(extractedLines.head.asSeq.head.coord))
                val holes = extractedLines.tail
                    .map({ h: MosaicLineStringJTS => h.asSeq.map(_.coord).toArray ++ Array(h.asSeq.head.coord) })
                    .map(gf.createLinearRing)
                    .toArray
                gf.createPolygon(exteriorRing, holes)
            case other: GeometryTypeEnum.Value => throw new UnsupportedOperationException(
                  s"MosaicGeometry.fromSeq() cannot create ${geomType.toString} from ${other.toString} geometries."
                )
        }
        newGeom.setSRID(spatialReference)
        MosaicPolygonJTS(newGeom)
    }

    def apply(geometry: Geometry): MosaicPolygonJTS = {
        new MosaicPolygonJTS(geometry.asInstanceOf[Polygon])
    }

    override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryJTS.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryJTS.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryJTS.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometry = MosaicGeometryJTS.fromHEX(hex)

}
