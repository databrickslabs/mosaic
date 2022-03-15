package com.databricks.labs.mosaic.core.geometry.linestring

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.multilinestring.MosaicMultiLineStringESRI
import com.databricks.labs.mosaic.core.geometry.point.{MosaicPoint, MosaicPointESRI}
import com.databricks.labs.mosaic.core.geometry.polygon.MosaicPolygonESRI
import com.databricks.labs.mosaic.core.types.model._
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.LINESTRING
import com.esri.core.geometry.ogc.{OGCGeometry, OGCLineString}

import org.apache.spark.sql.catalyst.InternalRow

class MosaicLineStringESRI(lineString: OGCLineString) extends MosaicGeometryESRI(lineString) with MosaicLineString {

    def this() = this(null)

    override def asSeq: Seq[MosaicPoint] = getBoundaryPoints

    override def toInternal: InternalGeometry = {
        val shell = for (i <- 0 until lineString.numPoints()) yield {
            val point = lineString.pointN(i)
            InternalCoord(MosaicPointESRI(point).coord)
        }
        new InternalGeometry(LINESTRING.id, Array(shell.toArray), Array(Array(Array())))
    }

    override def getBoundary: Seq[MosaicPoint] = getBoundaryPoints

    override def getBoundaryPoints: Seq[MosaicPoint] = {
        MosaicLineStringESRI.getPoints(lineString)
    }

    override def getHoles: Seq[Seq[MosaicPoint]] = getHolePoints

    override def getHolePoints: Seq[Seq[MosaicPoint]] = Nil

    override def getLength: Double = lineString.length()

    override def flatten: Seq[MosaicGeometry] = List(this)

    override def numPoints: Int = lineString.numPoints()

}

object MosaicLineStringESRI extends GeometryReader {

    def getPoints(lineString: OGCLineString): Seq[MosaicPoint] = {
        for (i <- 0 until lineString.numPoints()) yield MosaicPointESRI(lineString.pointN(i))
    }

    override def fromInternal(row: InternalRow): MosaicGeometry = {
        val internalGeom = InternalGeometry(row)
        val polyline = MosaicMultiLineStringESRI.createPolyline(internalGeom.boundaries)
        val ogcLineString = new OGCLineString(polyline, 0, MosaicGeometryESRI.spatialReference)
        MosaicLineStringESRI(ogcLineString)
    }

    def apply(ogcGeometry: OGCGeometry): MosaicLineStringESRI = {
        new MosaicLineStringESRI(ogcGeometry.asInstanceOf[OGCLineString])
    }

    override def fromPoints(points: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value = LINESTRING): MosaicGeometry = {
        require(geomType.id == LINESTRING.id)
        val polygon = MosaicPolygonESRI.fromPoints(points).asInstanceOf[MosaicGeometryESRI].getGeom
        MosaicLineStringESRI(polygon.boundary().asInstanceOf[OGCLineString])
    }

    override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryESRI.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryESRI.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryESRI.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometry = MosaicGeometryESRI.fromHEX(hex)

    override def fromKryo(row: InternalRow): MosaicGeometry = MosaicGeometryESRI.fromKryo(row)

}
