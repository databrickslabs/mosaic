package com.databricks.labs.mosaic.core.geometry.polygon

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.MosaicGeometryESRI.defaultSpatialReference
import com.databricks.labs.mosaic.core.geometry.linestring.{MosaicLineString, MosaicLineStringESRI}
import com.databricks.labs.mosaic.core.geometry.multipolygon.MosaicMultiPolygonESRI
import com.databricks.labs.mosaic.core.geometry.point.{MosaicPoint, MosaicPointESRI}
import com.databricks.labs.mosaic.core.types.model.{GeometryTypeEnum, _}
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.POLYGON
import com.esri.core.geometry.ogc._
import com.esri.core.geometry.SpatialReference

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

    override def getBoundary: MosaicGeometry = MosaicGeometryESRI(polygon.boundary())

    override def getLength: Double = MosaicGeometryESRI(polygon.boundary()).getLength

    override def numPoints: Int = {
        getHolePoints.map(_.length).sum + getShellPoints.map(_.length).sum
    }

    override def getShells: Seq[MosaicLineString] = Seq(MosaicLineStringESRI(polygon.exteriorRing()))

    override def getHoles: Seq[Seq[MosaicLineString]] =
        Seq(
          for (i <- 0 until polygon.numInteriorRing()) yield MosaicLineStringESRI(polygon.interiorRingN(i))
        )

    override def mapXY(f: (Double, Double) => (Double, Double)): MosaicGeometry = {
        def toInternalCoordArray(s: Seq[MosaicGeometry]): Array[InternalCoord] =
            s.map(g => InternalCoord(g.asInstanceOf[MosaicPointESRI].asSeq)).toArray
        val sr = SpatialReference.create(getSpatialReference)
        val shells = Array(toInternalCoordArray(getShells.map(_.mapXY(f))))
        val holes = Array(getHoles.map(_.map(_.mapXY(f))).map(toInternalCoordArray).toArray)
        val mosaicPolygon = MosaicMultiPolygonESRI.createPolygon(shells, holes)
        MosaicGeometryESRI(new OGCPolygon(mosaicPolygon, sr))
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
        MosaicGeometryESRI(new OGCPolygon(polygon, defaultSpatialReference))
    }

    override def fromPoints(inPoints: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value = POLYGON): MosaicGeometry = {
        require(geomType.id == POLYGON.id)
        val sr = SpatialReference.create(inPoints.head.getSpatialReference)
        val boundary = inPoints.map(_.coord).map(InternalCoord(_)).toArray
        val polygon = MosaicMultiPolygonESRI.createPolygon(Array(boundary), Array(Array(Array())))
        MosaicGeometryESRI(new OGCPolygon(polygon, sr))
    }

    override def fromLines(lines: Seq[MosaicLineString], geomType: GeometryTypeEnum.Value): MosaicGeometry = {
        require(geomType.id == POLYGON.id)
        val sr = SpatialReference.create(lines.head.getSpatialReference)
        val boundary = lines.head.asSeq.map(_.coord).map(InternalCoord(_)).toArray
        val holes = lines.tail.map({ h: MosaicLineString => h.asSeq.map(_.coord).map(InternalCoord(_)).toArray }).toArray
        val polygon = MosaicMultiPolygonESRI.createPolygon(Array(boundary), Array(holes))
        MosaicGeometryESRI(new OGCPolygon(polygon, sr))
    }

    override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryESRI.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryESRI.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryESRI.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometry = MosaicGeometryESRI.fromHEX(hex)

    override def fromKryo(row: InternalRow): MosaicGeometry = MosaicGeometryESRI.fromKryo(row)

}
