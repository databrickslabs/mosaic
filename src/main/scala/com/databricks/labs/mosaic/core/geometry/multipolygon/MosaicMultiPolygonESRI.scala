package com.databricks.labs.mosaic.core.geometry.multipolygon

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.multilinestring.MosaicMultiLineStringESRI
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint
import com.databricks.labs.mosaic.core.geometry.polygon.{MosaicPolygon, MosaicPolygonESRI}
import com.databricks.labs.mosaic.core.types.model._
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.MULTIPOLYGON
import com.esri.core.geometry.Polygon
import com.esri.core.geometry.ogc.{OGCGeometry, OGCMultiPolygon}

import org.apache.spark.sql.catalyst.InternalRow

class MosaicMultiPolygonESRI(multiPolygon: OGCMultiPolygon) extends MosaicGeometryESRI(multiPolygon) with MosaicMultiPolygon {

    override def toInternal: InternalGeometry = {
        val n = multiPolygon.numGeometries()
        val polygons = for (i <- 0 until n) yield MosaicPolygonESRI(multiPolygon.geometryN(i)).toInternal
        val boundaries = polygons.map(_.boundaries.head).toArray
        val holes = polygons.flatMap(_.holes).toArray
        new InternalGeometry(MULTIPOLYGON.id, boundaries, holes)
    }

    override def getBoundary: Seq[MosaicPoint] = getBoundaryPoints

    override def getHoles: Seq[Seq[MosaicPoint]] = getHolePoints

    override def getHolePoints: Seq[Seq[MosaicPoint]] = {
        val n = multiPolygon.numGeometries()
        val holeGroups = for (i <- 0 until n) yield MosaicPolygonESRI(multiPolygon.geometryN(i)).getHolePoints
        holeGroups.reduce(_ ++ _)
    }

    override def getLength: Double = MosaicGeometryESRI(multiPolygon.boundary()).getLength

    override def flatten: Seq[MosaicGeometry] = asSeq

    override def asSeq: Seq[MosaicGeometry] =
        for (i <- 0 until multiPolygon.numGeometries()) yield MosaicGeometryESRI(multiPolygon.geometryN(i))

    override def numPoints: Int = getHolePoints.length + getBoundaryPoints.length

    override def getBoundaryPoints: Seq[MosaicPoint] = {
        val n = multiPolygon.numGeometries()
        val boundaries = for (i <- 0 until n) yield MosaicPolygonESRI(multiPolygon.geometryN(i)).getBoundaryPoints
        boundaries.reduce(_ ++ _)
    }

    override def getPolygons: Seq[MosaicPolygon] = {
        val n = multiPolygon.numGeometries()
        for (i <- 0 until n) yield MosaicPolygonESRI(multiPolygon.geometryN(i))
    }

    override def mapCoords(f: MosaicPoint => MosaicPoint): MosaicGeometry = {
//        getPolygons.map {p: MosaicPolygon => p.mapCoords(f)}.asInstanceOf[MosaicMultiPolygonESRI]
    }

}

object MosaicMultiPolygonESRI extends GeometryReader {

    override def fromInternal(row: InternalRow): MosaicGeometry = {
        val internalGeom = InternalGeometry(row)
        val polygon = createPolygon(internalGeom.boundaries, internalGeom.holes)
        val ogcMultiLineString = new OGCMultiPolygon(polygon, MosaicGeometryESRI.spatialReference)
        MosaicMultiPolygonESRI(ogcMultiLineString)
    }

    // noinspection ZeroIndexToHead
    def createPolygon(shellCollection: Array[Array[InternalCoord]], holesCollection: Array[Array[Array[InternalCoord]]]): Polygon = {
        val boundariesPath = MosaicMultiLineStringESRI.createPolyline(shellCollection, dontClose = true)
        val holesPathsCollection = holesCollection.map(MosaicMultiLineStringESRI.createPolyline(_, dontClose = true))

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

    def apply(multiPolygon: OGCGeometry): MosaicMultiPolygonESRI = new MosaicMultiPolygonESRI(multiPolygon.asInstanceOf[OGCMultiPolygon])

    override def fromPoints(points: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value): MosaicGeometry = {
        throw new UnsupportedOperationException("fromPoints is not intended for creating MultiPolygons")
    }

    override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryESRI.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryESRI.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryESRI.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometry = MosaicGeometryESRI.fromHEX(hex)

    override def fromKryo(row: InternalRow): MosaicGeometry = MosaicGeometryESRI.fromKryo(row)

}
