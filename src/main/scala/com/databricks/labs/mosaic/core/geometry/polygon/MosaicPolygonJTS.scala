package com.databricks.labs.mosaic.core.geometry.polygon

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.linestring.{MosaicLineString, MosaicLineStringJTS}
import com.databricks.labs.mosaic.core.geometry.multipolygon.MosaicMultiPolygonJTS
import com.databricks.labs.mosaic.core.geometry.point.{MosaicPoint, MosaicPointJTS}
import com.databricks.labs.mosaic.core.types.model.{GeometryTypeEnum, _}
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.POLYGON
import com.esotericsoftware.kryo.io.Input
import org.locationtech.jts.geom._

import org.apache.spark.sql.catalyst.InternalRow

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

    override def mapXY(f: (Double, Double) => (Double, Double)): MosaicGeometry = {
        val shellTransformed = getShells.head.asInstanceOf[MosaicLineStringJTS].mapXY(f).asInstanceOf[MosaicLineStringJTS]
        val holesTransformed = getHoles.head.map(_.asInstanceOf[MosaicLineStringJTS].mapXY(f).asInstanceOf[MosaicLineStringJTS])
        val newGeom = MosaicPolygonJTS.fromLines(Seq(shellTransformed) ++ holesTransformed)
        newGeom.setSpatialReference(getSpatialReference)
        newGeom
    }

    override def asSeq: Seq[MosaicLineString] = getShells ++ getHoles.flatten

}

object MosaicPolygonJTS extends GeometryReader {

    def apply(geometry: Geometry): MosaicPolygonJTS = {
        new MosaicPolygonJTS(geometry.asInstanceOf[Polygon])
    }

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

    override def fromPoints(points: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value = POLYGON): MosaicGeometry = {
        require(geomType.id == POLYGON.id)
        val gf = new GeometryFactory()
        val shell = points.map(_.coord).toArray ++ Array(points.head.coord)
        val polygon = gf.createPolygon(shell)
        polygon.setSRID(points.head.getSpatialReference)
        new MosaicPolygonJTS(polygon)
    }

    override def fromLines(lines: Seq[MosaicLineString], geomType: GeometryTypeEnum.Value = POLYGON): MosaicGeometry = {
        require(geomType.id == POLYGON.id)
        val gf = new GeometryFactory()
        val sr = lines.head.getSpatialReference
        val shell = gf.createLinearRing(lines.head.asSeq.map(_.coord).toArray ++ Array(lines.head.asSeq.head.coord))
        val holes = lines.tail
            .map({ h: MosaicLineString => h.asSeq.map(_.coord).toArray ++ Array(h.asSeq.head.coord) })
            .map(gf.createLinearRing)
            .toArray
        val polygon = gf.createPolygon(shell, holes)
        polygon.setSRID(sr)
        MosaicGeometryJTS(polygon)
    }

    override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryJTS.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryJTS.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryJTS.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometry = MosaicGeometryJTS.fromHEX(hex)

    override def fromKryo(row: InternalRow): MosaicGeometry = {
        val kryoBytes = row.getBinary(1)
        val input = new Input(kryoBytes)
        MosaicGeometryJTS.kryo.readObject(input, classOf[MosaicPolygonJTS])
    }

}
