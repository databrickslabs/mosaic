package com.databricks.labs.mosaic.core.geometry.polygon

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.MosaicGeometryESRI.spatialReference
import com.databricks.labs.mosaic.core.geometry.multipolygon.MosaicMultiPolygonESRI
import com.databricks.labs.mosaic.core.geometry.point.{MosaicPoint, MosaicPointESRI}
import com.databricks.labs.mosaic.core.types.model._
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.POLYGON
import com.esri.core.geometry.ogc._

import org.apache.spark.sql.catalyst.InternalRow

class MosaicPolygonESRI(polygon: OGCPolygon) extends MosaicGeometryESRI(polygon) with MosaicPolygon {

    def this() = this(null)

    override def toInternal: InternalGeometry = {
        def ringToInternalCoords(ring: OGCLineString): Array[InternalCoord] = {
            for (i <- 0 until ring.numPoints()) yield InternalCoord(MosaicPointESRI(ring.pointN(i)).coord)
        }.toArray

        val boundary = polygon.boundary().geometryN(0).asInstanceOf[OGCLineString]
        val shell = ringToInternalCoords(boundary)
        val holes = for (i <- 0 until polygon.numInteriorRing()) yield ringToInternalCoords(polygon.interiorRingN(i))

        new InternalGeometry(POLYGON.id, Array(shell), Array(holes.toArray))
    }

    override def getBoundary: Seq[MosaicPoint] = getBoundaryPoints

    override def getHoles: Seq[Seq[MosaicPoint]] = getHolePoints

    override def getLength: Double = MosaicGeometryESRI(polygon.boundary()).getLength

    override def flatten: Seq[MosaicGeometry] = List(this)

    override def numPoints: Int = getHolePoints.length + getBoundaryPoints.length

    override def getBoundaryPoints: Seq[MosaicPoint] = {
        MosaicPolygonESRI.getPoints(polygon.exteriorRing())
    }

    override def getHolePoints: Seq[Seq[MosaicPoint]] = {
        for (i <- 0 until polygon.numInteriorRing()) yield MosaicPolygonESRI.getPoints(polygon.interiorRingN(i))
    }

}

object MosaicPolygonESRI extends GeometryReader {

    def apply(ogcGeometry: OGCGeometry): MosaicPolygonESRI = {
        new MosaicPolygonESRI(ogcGeometry.asInstanceOf[OGCPolygon])
    }

    def getPoints(lineString: OGCLineString): Seq[MosaicPoint] = {
        for (i <- 0 until lineString.numPoints()) yield MosaicPointESRI(lineString.pointN(i))
    }

    override def fromInternal(row: InternalRow): MosaicGeometry = {
        val internalGeom = InternalGeometry(row)
        val polygon = MosaicMultiPolygonESRI.createPolygon(internalGeom.boundaries, internalGeom.holes)
        MosaicGeometryESRI(new OGCPolygon(polygon, spatialReference))
    }

    override def fromPoints(inPoints: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value = POLYGON): MosaicGeometry = {
        require(geomType.id == POLYGON.id)
        val boundary = inPoints.map(_.coord).map(InternalCoord(_)).toArray
        val polygon = MosaicMultiPolygonESRI.createPolygon(Array(boundary), Array(Array(Array())))
        MosaicGeometryESRI(new OGCPolygon(polygon, spatialReference))
    }

    override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryESRI.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryESRI.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryESRI.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometry = MosaicGeometryESRI.fromHEX(hex)

    override def fromKryo(row: InternalRow): MosaicGeometry = MosaicGeometryESRI.fromKryo(row)

}
