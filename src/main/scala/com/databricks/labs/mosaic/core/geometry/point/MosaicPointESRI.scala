package com.databricks.labs.mosaic.core.geometry.point

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.types.model._
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.POINT
import com.esri.core.geometry.{Point, SpatialReference}
import com.esri.core.geometry.ogc.{OGCGeometry, OGCPoint}
import com.databricks.labs.mosaic.core.types.model.GeoCoord
import org.locationtech.jts.geom.Coordinate

import org.apache.spark.sql.catalyst.InternalRow

class MosaicPointESRI(point: OGCPoint) extends MosaicGeometryESRI(point) with MosaicPoint {

    def this() = this(null)

    override def geoCoord: GeoCoord = GeoCoord(point.Y(), point.X())

    override def asSeq: Seq[Double] =
        if (point.is3D()) {
            Seq(getX, getY, getZ)
        } else {
            Seq(getX, getY)
        }

    override def getZ: Double = point.Z()

    override def toInternal: InternalGeometry = {
        val shell = Array(InternalCoord(coord))
        new InternalGeometry(POINT.id, getSpatialReference, Array(shell), Array(Array(Array())))
    }

    override def coord: Coordinate = {
        if (point.is3D()) {
            new Coordinate(point.X(), point.Y(), point.Z())
        } else {
            new Coordinate(point.X(), point.Y())
        }
    }

    override def getBoundary: MosaicGeometry = MosaicGeometryESRI(point.boundary())

    override def getLength: Double = 0.0

    override def numPoints: Int = 1

    override def mapXY(f: (Double, Double) => (Double, Double)): MosaicGeometry = {
        val (x_, y_) = f(getX, getY)
        MosaicPointESRI(
          new OGCPoint(new Point(x_, y_), point.getEsriSpatialReference)
        )
    }

    override def getX: Double = point.X()

    override def getY: Double = point.Y()

}

object MosaicPointESRI extends GeometryReader {

    def apply(geoCoord: GeoCoord): MosaicPointESRI = {
        MosaicPointESRI(
          new OGCPoint(new Point(geoCoord.lng, geoCoord.lat), SpatialReference.create(defaultSpatialReferenceId))
        )
    }

    def apply(coords: Seq[Double]): MosaicPointESRI = {
        if (coords.length == 3) {
            MosaicPointESRI(
              new OGCPoint(new Point(coords(0), coords(1), coords(2)), SpatialReference.create(defaultSpatialReferenceId))
            )
        } else {
            MosaicPointESRI(
              new OGCPoint(new Point(coords(0), coords(1)), SpatialReference.create(defaultSpatialReferenceId))
            )
        }
    }

    def apply(point: OGCGeometry): MosaicPointESRI = new MosaicPointESRI(point.asInstanceOf[OGCPoint])

    // noinspection ZeroIndexToHead
    override def fromInternal(row: InternalRow): MosaicGeometry = {
        val internalGeom = InternalGeometry(row)
        require(internalGeom.typeId == POINT.id)

        val coordinate = internalGeom.boundaries.head.head
        val spatialReference =
            if (internalGeom.srid != 0) {
                SpatialReference.create(internalGeom.srid)
            } else {
                MosaicGeometryESRI.defaultSpatialReference
            }
        val point =
            if (coordinate.coords.length == 2) {
                new OGCPoint(new Point(coordinate.coords(0), coordinate.coords(1)), spatialReference)
            } else {
                new OGCPoint(
                  new Point(coordinate.coords(0), coordinate.coords(1), coordinate.coords(2)),
                  spatialReference
                )
            }
        new MosaicPointESRI(point)
    }

    override def fromSeq[T <: MosaicGeometry](geomSeq: Seq[T], geomType: GeometryTypeEnum.Value = POINT): MosaicPointESRI = {
        val spatialReference = SpatialReference.create(geomSeq.head.getSpatialReference)
        val newGeom = GeometryTypeEnum.fromString(geomSeq.head.getGeometryType) match {
            case POINT                         =>
                val extractedPoint = geomSeq.head.asInstanceOf[MosaicPoint]
                extractedPoint.asSeq.length match {
                    case 3 => new OGCPoint(new Point(extractedPoint.getX, extractedPoint.getY, extractedPoint.getZ), spatialReference)
                    case 2 => new OGCPoint(new Point(extractedPoint.getX, extractedPoint.getY), spatialReference)
                }
            case other: GeometryTypeEnum.Value => throw new UnsupportedOperationException(
                  s"MosaicGeometry.fromSeq() cannot create ${geomType.toString} from ${other.toString} geometries."
                )
        }
        MosaicPointESRI(newGeom)
    }

    override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryESRI.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryESRI.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryESRI.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometry = MosaicGeometryESRI.fromHEX(hex)

}
