package com.databricks.mosaic.core.geometry.multipolygon

import com.esri.core.geometry.Polygon
import com.esri.core.geometry.ogc.{OGCGeometry, OGCMultiPolygon}

import org.apache.spark.sql.catalyst.InternalRow

import com.databricks.mosaic.core.geometry._
import com.databricks.mosaic.core.geometry.multilinestring.MosaicMultiLineStringOGC
import com.databricks.mosaic.core.geometry.point.MosaicPoint
import com.databricks.mosaic.core.geometry.polygon.MosaicPolygonOGC
import com.databricks.mosaic.core.types.model._
import com.databricks.mosaic.core.types.model.GeometryTypeEnum.MULTIPOLYGON

class MosaicMultiPolygonOGC(multiPolygon: OGCMultiPolygon) extends MosaicGeometryOGC(multiPolygon) with MosaicMultiPolygon {

    override def toInternal: InternalGeometry = {
        val n = multiPolygon.numGeometries()
        val polygons = for (i <- 0 until n) yield MosaicPolygonOGC(multiPolygon.geometryN(i)).toInternal
        val boundaries = polygons.map(_.boundaries.head).toArray
        val holes = polygons.flatMap(_.holes).toArray
        new InternalGeometry(MULTIPOLYGON.id, boundaries, holes)
    }

    override def getBoundary: Seq[MosaicPoint] = getBoundaryPoints

    override def getBoundaryPoints: Seq[MosaicPoint] = {
        val n = multiPolygon.numGeometries()
        val boundaries = for (i <- 0 until n) yield MosaicPolygonOGC(multiPolygon.geometryN(i)).getBoundaryPoints
        boundaries.reduce(_ ++ _)
    }

    override def getHoles: Seq[Seq[MosaicPoint]] = getHolePoints

    override def getHolePoints: Seq[Seq[MosaicPoint]] = {
        val n = multiPolygon.numGeometries()
        val holeGroups = for (i <- 0 until n) yield MosaicPolygonOGC(multiPolygon.geometryN(i)).getHolePoints
        holeGroups.reduce(_ ++ _)
    }

    override def getLength: Double = MosaicGeometryOGC(multiPolygon.boundary()).getLength

    override def flatten: Seq[MosaicGeometry] = asSeq

    override def asSeq: Seq[MosaicGeometry] =
        for (i <- 0 until multiPolygon.numGeometries()) yield MosaicGeometryOGC(multiPolygon.geometryN(i))

    override def numPoints: Int = getHolePoints.length + getBoundaryPoints.length
}

object MosaicMultiPolygonOGC extends GeometryReader {

    override def fromInternal(row: InternalRow): MosaicGeometry = {
        val internalGeom = InternalGeometry(row)
        val polygon = createPolygon(internalGeom.boundaries, internalGeom.holes)
        val ogcMultiLineString = new OGCMultiPolygon(polygon, MosaicGeometryOGC.spatialReference)
        MosaicMultiPolygonOGC(ogcMultiLineString)
    }

    // noinspection ZeroIndexToHead
    def createPolygon(shellCollection: Array[Array[InternalCoord]], holesCollection: Array[Array[Array[InternalCoord]]]): Polygon = {
        val boundariesPath = MosaicMultiLineStringOGC.createPolyline(shellCollection, dontClose = true)
        val holesPathsCollection = holesCollection.map(MosaicMultiLineStringOGC.createPolyline(_, dontClose = true))

        val polygon = new Polygon()

        for (i <- 0 until boundariesPath.getPathCount) {
            val tmpPolygon = new Polygon()
            tmpPolygon.addPath(boundariesPath, i, true)
            if (tmpPolygon.calculateArea2D() < 0) {
                polygon.addPath(boundariesPath, i, false)
            } else {
                polygon.addPath(boundariesPath, i, true)
            }
        }
        holesPathsCollection.foreach(holesPath =>
            for (i <- 0 until holesPath.getPathCount) {
                val tmpPolygon = new Polygon()
                tmpPolygon.addPath(holesPath, i, true)
                if (tmpPolygon.calculateArea2D() < 0) {
                    polygon.addPath(holesPath, i, true)
                } else {
                    polygon.addPath(holesPath, i, false)
                }
            }
        )

        polygon
    }

    def apply(multiPolygon: OGCGeometry): MosaicMultiPolygonOGC = new MosaicMultiPolygonOGC(multiPolygon.asInstanceOf[OGCMultiPolygon])

    override def fromPoints(points: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value): MosaicGeometry = {
        throw new UnsupportedOperationException("fromPoints is not intended for creating MultiPolygons")
    }

    override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryOGC.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryOGC.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryOGC.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometry = MosaicGeometryOGC.fromHEX(hex)

    override def fromKryo(row: InternalRow): MosaicGeometry = MosaicGeometryOGC.fromKryo(row)

}
