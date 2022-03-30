package com.databricks.labs.mosaic.core.geometry.multipoint

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.point.{MosaicPoint, MosaicPointESRI}
import com.databricks.labs.mosaic.core.types.model._
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.MULTIPOINT
import com.esri.core.geometry.{MultiPoint, Point, SpatialReference}
import com.esri.core.geometry.ogc._

import org.apache.spark.sql.catalyst.InternalRow

class MosaicMultiPointESRI(multiPoint: OGCMultiPoint) extends MosaicGeometryESRI(multiPoint) with MosaicMultiPoint {

    // noinspection DuplicatedCode
    override def toInternal: InternalGeometry = {
        val points = asSeq.map(_.coord).map(InternalCoord(_))
        new InternalGeometry(MULTIPOINT.id, Array(points.toArray), Array(Array(Array())))
    }

    override def asSeq: Seq[MosaicPoint] = {
        for (i <- 0 until multiPoint.numGeometries()) yield MosaicPointESRI(multiPoint.geometryN(i))
    }

    override def getBoundary: MosaicGeometry = MosaicGeometryESRI(multiPoint.boundary())

    override def getLength: Double = 0.0

    override def numPoints: Int = multiPoint.numGeometries()

    override def mapXY(f: (Double, Double) => (Double, Double)): MosaicGeometry = {
        MosaicMultiPointESRI.fromPoints(asSeq.map(_.mapXY(f).asInstanceOf[MosaicPointESRI]))
    }

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

        val ogcMultiPoint = new OGCMultiPoint(multiPoint, MosaicGeometryESRI.defaultSpatialReference)
        new MosaicMultiPointESRI(ogcMultiPoint)
    }

    // noinspection ZeroIndexToHead
    override def fromPoints(points: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value = MULTIPOINT): MosaicGeometry = {
        require(geomType.id == MULTIPOINT.id)
        fromPoints(points.map(_.asInstanceOf[MosaicPointESRI]))
    }

    private def fromPoints(points: Seq[MosaicPointESRI]): MosaicGeometry = {
        val multiPoint = new MultiPoint()
        val spatialReference = SpatialReference.create(points.head.getSpatialReference)
        val dim = points.head.asSeq.length

        dim match {
            case 2 => points.foreach(point => multiPoint.add(new Point(point.asSeq(0), point.asSeq(1))))
            case 3 => points.foreach(point => multiPoint.add(new Point(point.asSeq(0), point.asSeq(1), point.asSeq(2))))
            case _ => throw new UnsupportedOperationException("Only 2D and 3D points supported.")
        }

        val ogcMultiPoint = new OGCMultiPoint(multiPoint, spatialReference)
        new MosaicMultiPointESRI(ogcMultiPoint)

    }

    override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryESRI.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryESRI.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryESRI.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometry = MosaicGeometryESRI.fromHEX(hex)

    override def fromKryo(row: InternalRow): MosaicGeometry = MosaicGeometryESRI.fromKryo(row)

}
