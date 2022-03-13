package com.databricks.mosaic.core.geometry.point

import com.esri.core.geometry.{Point, SpatialReference}
import com.esri.core.geometry.ogc.{OGCGeometry, OGCPoint}
import com.uber.h3core.util.GeoCoord
import org.locationtech.jts.geom.Coordinate

import org.apache.spark.sql.catalyst.InternalRow

import com.databricks.mosaic.core.geometry._
import com.databricks.mosaic.core.types.model._
import com.databricks.mosaic.core.types.model.GeometryTypeEnum.POINT

class MosaicPointOGC(point: OGCPoint) extends MosaicGeometryOGC(point) with MosaicPoint {

    def this() = this(null)

    override def geoCoord: GeoCoord = new GeoCoord(point.Y(), point.X())

    override def asSeq: Seq[Double] =
        if (point.is3D()) {
            Seq(getX, getY, getZ)
        } else {
            Seq(getX, getY)
        }

    override def getX: Double = point.X()

    override def getY: Double = point.Y()

    override def getZ: Double = point.Z()

    override def toInternal: InternalGeometry = {
        val shell = Array(InternalCoord(coord))
        new InternalGeometry(POINT.id, Array(shell), Array(Array(Array())))
    }

    override def coord: Coordinate = {
        if (point.is3D()) {
            new Coordinate(point.X(), point.Y(), point.Z())
        } else {
            new Coordinate(point.X(), point.Y())
        }
    }

    override def getBoundary: Seq[MosaicPoint] = Seq(this)

    override def getHoles: Seq[Seq[MosaicPoint]] = Nil

    override def getLength: Double = 0.0

    override def flatten: Seq[MosaicGeometry] = List(this)

    override def numPoints: Int = 1
}

object MosaicPointOGC extends GeometryReader {

    def apply(geoCoord: GeoCoord): MosaicPointOGC = {
        MosaicPointOGC(
          new OGCPoint(new Point(geoCoord.lng, geoCoord.lat), SpatialReference.create(4326))
        )
    }

    def apply(point: OGCGeometry): MosaicPointOGC = new MosaicPointOGC(point.asInstanceOf[OGCPoint])

    // noinspection ZeroIndexToHead
    override def fromInternal(row: InternalRow): MosaicGeometry = {
        val internalGeom = InternalGeometry(row)
        require(internalGeom.typeId == POINT.id)

        val coordinate = internalGeom.boundaries.head.head
        val point =
            if (coordinate.coords.length == 2) {
                new OGCPoint(new Point(coordinate.coords(0), coordinate.coords(1)), MosaicGeometryOGC.spatialReference)
            } else {
                new OGCPoint(
                  new Point(coordinate.coords(0), coordinate.coords(1), coordinate.coords(2)),
                  MosaicGeometryOGC.spatialReference
                )
            }
        new MosaicPointOGC(point)
    }

    override def fromPoints(points: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value = POINT): MosaicGeometry = {
        require(geomType.id == POINT.id)

        val mosaicPoint = points.head
        val point =
            if (mosaicPoint.asSeq.length == 2) {
                new OGCPoint(new Point(mosaicPoint.getX, mosaicPoint.getY), MosaicGeometryOGC.spatialReference)
            } else {
                new OGCPoint(new Point(mosaicPoint.getX, mosaicPoint.getY, mosaicPoint.getZ), MosaicGeometryOGC.spatialReference)
            }
        new MosaicPointOGC(point)
    }

    override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryOGC.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryOGC.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryOGC.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometry = MosaicGeometryOGC.fromHEX(hex)

    override def fromKryo(row: InternalRow): MosaicGeometry = MosaicGeometryOGC.fromKryo(row)

}
