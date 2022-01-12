package com.databricks.mosaic.core.geometry.multipoint

import com.esotericsoftware.kryo.io.Input
import org.locationtech.jts.geom._

import org.apache.spark.sql.catalyst.InternalRow

import com.databricks.mosaic.core.geometry._
import com.databricks.mosaic.core.geometry.point.{MosaicPoint, MosaicPointJTS}
import com.databricks.mosaic.core.geometry.polygon.{MosaicPolygon, MosaicPolygonJTS}
import com.databricks.mosaic.core.types.model._
import com.databricks.mosaic.core.types.model.GeometryTypeEnum.MULTIPOINT

class MosaicMultiPointJTS(multiPoint: MultiPoint) extends MosaicGeometryJTS(multiPoint) with MosaicMultiPoint {

    //noinspection DuplicatedCode
    override def toInternal: InternalGeometry = {
        val points = asSeq.map(_.coord).map(InternalCoord(_))
        new InternalGeometry(MULTIPOINT.id, Array(points.toArray), Array(Array(Array())))
    }

    override def getBoundary: Seq[MosaicPoint] = asSeq

    override def getHoles: Seq[Seq[MosaicPoint]] = Nil

    override def flatten: Seq[MosaicGeometry] = asSeq

    override def asSeq: Seq[MosaicPoint] = {
        for (i <- 0 until multiPoint.getNumPoints) yield MosaicPointJTS(multiPoint.getGeometryN(i).getCoordinates.head)
    }

    override def convexHull: MosaicPolygon = new MosaicPolygonJTS(multiPoint.convexHull.asInstanceOf[Polygon])

}

object MosaicMultiPointJTS extends GeometryReader {

    def apply(geom: Geometry): MosaicMultiPointJTS = new MosaicMultiPointJTS(geom.asInstanceOf[MultiPoint])

    // noinspection ZeroIndexToHead
    override def fromInternal(row: InternalRow): MosaicGeometry = {
        val gf = new GeometryFactory()
        val internalGeom = InternalGeometry(row)
        require(internalGeom.typeId == MULTIPOINT.id)

        val points = internalGeom.boundaries.head.map(p => gf.createPoint(p.toCoordinate))
        val multiPoint = gf.createMultiPoint(points)
        new MosaicMultiPointJTS(multiPoint)
    }

    // noinspection ZeroIndexToHead
    override def fromPoints(inPoints: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value = MULTIPOINT): MosaicGeometry = {
        val gf = new GeometryFactory()
        require(geomType.id == MULTIPOINT.id)

        val points = inPoints.map(p => gf.createPoint(p.coord)).toArray
        val multiPoint = gf.createMultiPoint(points)
        new MosaicMultiPointJTS(multiPoint)
    }

    override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryJTS.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryJTS.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryJTS.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometry = MosaicGeometryJTS.fromHEX(hex)

    override def fromKryo(row: InternalRow): MosaicGeometry = {
        val kryoBytes = row.getBinary(1)
        val input = new Input(kryoBytes)
        MosaicGeometryJTS.kryo.readObject(input, classOf[MosaicMultiPointJTS])
    }

}
