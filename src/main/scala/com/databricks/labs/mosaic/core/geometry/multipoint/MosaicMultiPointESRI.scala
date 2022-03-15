package com.databricks.labs.mosaic.core.geometry.multipoint

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.point.{MosaicPoint, MosaicPointESRI}
import com.databricks.labs.mosaic.core.types.model._
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.MULTIPOINT
import com.esri.core.geometry.{MultiPoint, Point}
import com.esri.core.geometry.ogc._

import org.apache.spark.sql.catalyst.InternalRow

class MosaicMultiPointESRI(multiPoint: OGCMultiPoint) extends MosaicGeometryESRI(multiPoint) with MosaicMultiPoint {

    // noinspection DuplicatedCode
    override def toInternal: InternalGeometry = {
        val points = asSeq.map(_.coord).map(InternalCoord(_))
        new InternalGeometry(MULTIPOINT.id, Array(points.toArray), Array(Array(Array())))
    }

    override def getBoundary: Seq[MosaicPoint] = asSeq

    override def asSeq: Seq[MosaicPoint] = {
        for (i <- 0 until multiPoint.numGeometries()) yield MosaicPointESRI(multiPoint.geometryN(i))
    }

    override def getHoles: Seq[Seq[MosaicPoint]] = Nil

    override def getLength: Double = 0.0

    override def flatten: Seq[MosaicGeometry] = asSeq

    override def numPoints: Int = multiPoint.numGeometries()

}

object MosaicMultiPointESRI extends GeometryReader {

    def apply(multiPoint: OGCGeometry): MosaicGeometryESRI = new MosaicMultiPointESRI(multiPoint.asInstanceOf[OGCMultiPoint])

    // noinspection ZeroIndexToHead
    override def fromInternal(row: InternalRow): MosaicGeometry = {
        val internalGeom = InternalGeometry(row)
        require(internalGeom.typeId == MULTIPOINT.id)

        val multiPoint = new MultiPoint()
        val coordsCollection = internalGeom.boundaries.head.map(_.coords)
        val dim = coordsCollection.head.length

        dim match {
            case 2 => coordsCollection.foreach(coords => multiPoint.add(new Point(coords(0), coords(1))))
            case 3 => coordsCollection.foreach(coords => multiPoint.add(new Point(coords(0), coords(1), coords(2))))
            case _ => throw new UnsupportedOperationException("Only 2D and 3D points supported.")
        }

        val ogcMultiPoint = new OGCMultiPoint(multiPoint, MosaicGeometryESRI.spatialReference)
        new MosaicMultiPointESRI(ogcMultiPoint)
    }

    // noinspection ZeroIndexToHead
    override def fromPoints(points: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value = MULTIPOINT): MosaicGeometry = {
        require(geomType.id == MULTIPOINT.id)

        val multiPoint = new MultiPoint()
        val dim = points.head.asSeq.length

        dim match {
            case 2 => points.foreach(point => multiPoint.add(new Point(point.asSeq(0), point.asSeq(1))))
            case 3 => points.foreach(point => multiPoint.add(new Point(point.asSeq(0), point.asSeq(1), point.asSeq(2))))
            case _ => throw new UnsupportedOperationException("Only 2D and 3D points supported.")
        }

        val ogcMultiPoint = new OGCMultiPoint(multiPoint, MosaicGeometryESRI.spatialReference)
        new MosaicMultiPointESRI(ogcMultiPoint)
    }

    override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryESRI.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryESRI.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryESRI.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometry = MosaicGeometryESRI.fromHEX(hex)

    override def fromKryo(row: InternalRow): MosaicGeometry = MosaicGeometryESRI.fromKryo(row)

}
