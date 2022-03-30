package com.databricks.labs.mosaic.core.geometry.polygon

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.linestring.{MosaicLineString, MosaicLineStringJTS}
import com.databricks.labs.mosaic.core.geometry.point.{MosaicPoint, MosaicPointJTS}
import com.databricks.labs.mosaic.core.types.model._
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.POLYGON
import com.esotericsoftware.kryo.io.Input
import org.locationtech.jts.geom._

import org.apache.spark.sql.catalyst.InternalRow

class MosaicPolygonJTS(polygon: Polygon) extends MosaicGeometryJTS(polygon) with MosaicPolygon {

    override def toInternal: InternalGeometry = {
        val boundary = polygon.getBoundary
        val shell = boundary.getGeometryN(0).getCoordinates.map(InternalCoord(_))
        val holes = for (i <- 1 until boundary.getNumGeometries) yield boundary.getGeometryN(i).getCoordinates.map(InternalCoord(_))
        new InternalGeometry(POLYGON.id, Array(shell), Array(holes.toArray))
    }

    override def getBoundary: MosaicGeometry = MosaicGeometryJTS(polygon.getBoundary)

    override def getShells: Seq[MosaicLineString] = Seq(MosaicLineStringJTS(polygon.getExteriorRing))

    override def getHoles: Seq[Seq[MosaicLineString]] =
        Seq(for (i <- 1 until polygon.getNumInteriorRing) yield MosaicLineStringJTS(polygon.getInteriorRingN(i)))

    override def mapXY(f: (Double, Double) => (Double, Double)): MosaicGeometry = {
        val gf = new GeometryFactory()
        val shell = gf.createLinearRing(
          getShells.head.mapXY(f).asInstanceOf[MosaicLineStringJTS].getGeom.asInstanceOf[LineString].getCoordinateSequence
        )
        val holes = getHoles.head
            .map(_.mapXY(f).asInstanceOf[MosaicLineStringJTS].getGeom.asInstanceOf[LineString].getCoordinateSequence)
            .map(gf.createLinearRing)
            .toArray
        val geom = gf.createPolygon(shell, holes)
        geom.setSRID(getSpatialReference)
        MosaicPolygonJTS(geom)
    }

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
        MosaicGeometryJTS(geometry)
    }

    override def fromPoints(points: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value = POLYGON): MosaicGeometry = {
        require(geomType.id == POLYGON.id)
        val gf = new GeometryFactory()
        val shell = points.map(_.coord).toArray
        val polygon = gf.createPolygon(shell)
        new MosaicPolygonJTS(polygon)
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
