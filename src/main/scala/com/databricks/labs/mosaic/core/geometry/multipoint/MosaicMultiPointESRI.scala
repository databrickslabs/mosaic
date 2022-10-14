package com.databricks.labs.mosaic.core.geometry.multipoint

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.point.{MosaicPoint, MosaicPointESRI}
import com.databricks.labs.mosaic.core.types.model._
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.{MULTIPOINT, POINT}
import com.esri.core.geometry._
import com.esri.core.geometry.ogc._
import org.apache.spark.sql.catalyst.InternalRow

class MosaicMultiPointESRI(multiPoint: OGCMultiPoint) extends MosaicGeometryESRI(multiPoint) with MosaicMultiPoint {

    // noinspection DuplicatedCode
    override def toInternal: InternalGeometry = {
        val points = asSeq.map(_.coord).map(InternalCoord(_))
        new InternalGeometry(MULTIPOINT.id, getSpatialReference, Array(points.toArray), Array(Array(Array())))
    }

    override def getBoundary: MosaicGeometry = MosaicGeometryESRI(multiPoint.boundary())

    override def getLength: Double = 0.0

    override def numPoints: Int = multiPoint.numGeometries()

    override def mapXY(f: (Double, Double) => (Double, Double)): MosaicGeometry = {
        MosaicMultiPointESRI.fromSeq(asSeq.map(_.mapXY(f).asInstanceOf[MosaicPointESRI]))
    }

    override def asSeq: Seq[MosaicPoint] = {
        for (i <- 0 until multiPoint.numGeometries()) yield MosaicPointESRI(multiPoint.geometryN(i))
    }

}

object MosaicMultiPointESRI extends GeometryReader {

    // noinspection ZeroIndexToHead
    override def fromInternal(row: InternalRow): MosaicGeometry = {
        val internalGeom = InternalGeometry(row)
        require(internalGeom.typeId == MULTIPOINT.id)

        val multiPoint = new MultiPoint()
        val coordsCollection = internalGeom.boundaries.head.map(_.coords)
        val dim = coordsCollection.head.length
        val spatialReference =
            if (internalGeom.srid != 0) {
                SpatialReference.create(internalGeom.srid)
            } else {
                MosaicGeometryESRI.defaultSpatialReference
            }

        dim match {
            case 2 => coordsCollection.foreach(coords => multiPoint.add(new Point(coords(0), coords(1))))
            case 3 => coordsCollection.foreach(coords => multiPoint.add(new Point(coords(0), coords(1), coords(2))))
            case _ => throw new UnsupportedOperationException("Only 2D and 3D points supported.")
        }

        val ogcMultiPoint = new OGCMultiPoint(multiPoint, spatialReference)
        new MosaicMultiPointESRI(ogcMultiPoint)
    }

    override def fromSeq[T <: MosaicGeometry](geomSeq: Seq[T], geomType: GeometryTypeEnum.Value = MULTIPOINT): MosaicMultiPointESRI = {
        val spatialReference = SpatialReference.create(geomSeq.head.getSpatialReference)
        val newGeom = GeometryTypeEnum.fromString(geomSeq.head.getGeometryType) match {
            case POINT                         =>
                val multiPoint = new MultiPoint()
                val extractedPoints = geomSeq.map(_.asInstanceOf[MosaicPointESRI])
                extractedPoints.head.asSeq.length match {
                    case 2 => extractedPoints.foreach(p => multiPoint.add(new Point(p.asSeq(0), p.asSeq(1))))
                    case 3 => extractedPoints.foreach(p => multiPoint.add(new Point(p.asSeq(0), p.asSeq(1), p.asSeq(2))))
                }
                new OGCMultiPoint(multiPoint, spatialReference)
            case other: GeometryTypeEnum.Value => throw new UnsupportedOperationException(
                  s"MosaicGeometry.fromSeq() cannot create ${geomType.toString} from ${other.toString} geometries."
                )
        }
        MosaicMultiPointESRI(newGeom)
    }

    def apply(multiPoint: OGCGeometry): MosaicMultiPointESRI = new MosaicMultiPointESRI(multiPoint.asInstanceOf[OGCMultiPoint])

    override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryESRI.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryESRI.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryESRI.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometry = MosaicGeometryESRI.fromHEX(hex)

}
