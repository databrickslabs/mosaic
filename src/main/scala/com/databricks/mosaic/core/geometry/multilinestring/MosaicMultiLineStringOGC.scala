package com.databricks.mosaic.core.geometry.multilinestring

import com.esri.core.geometry.Polyline
import com.esri.core.geometry.ogc._

import org.apache.spark.sql.catalyst.InternalRow

import com.databricks.mosaic.core.geometry._
import com.databricks.mosaic.core.geometry.linestring.{MosaicLineString, MosaicLineStringOGC}
import com.databricks.mosaic.core.geometry.point.MosaicPoint
import com.databricks.mosaic.core.types.model._
import com.databricks.mosaic.core.types.model.GeometryTypeEnum.MULTILINESTRING

class MosaicMultiLineStringOGC(multiLineString: OGCMultiLineString) extends MosaicGeometryOGC(multiLineString) with MosaicMultiLineString {

    override def getHolePoints: Seq[Seq[Seq[MosaicPoint]]] = Nil

    override def getBoundaryPoints: Seq[Seq[MosaicPoint]] = {
        for (i <- 0 until multiLineString.numGeometries()) yield {
            val lineString = multiLineString.geometryN(i).asInstanceOf[OGCLineString]
            MosaicLineStringOGC.getPoints(lineString)
        }
    }

    override def toInternal: InternalGeometry = {
        val shells = for (i <- 0 until multiLineString.numGeometries()) yield {
            val lineString = multiLineString.geometryN(i).asInstanceOf[OGCLineString]
            MosaicLineStringOGC(lineString).toInternal.boundaries.head
        }
        new InternalGeometry(MULTILINESTRING.id, shells.toArray, Array(Array(Array())))
    }

    override def getLength: Double = multiLineString.length()

    override def getBoundary: Seq[MosaicPoint] = MosaicGeometryOGC(multiLineString.boundary()).getBoundary

    override def getHoles: Seq[Seq[MosaicPoint]] = Nil

    override def flatten: Seq[MosaicGeometry] = asSeq

    override def asSeq: Seq[MosaicLineString] =
        for (i <- 0 until multiLineString.numGeometries())
            yield new MosaicLineStringOGC(multiLineString.geometryN(i).asInstanceOf[OGCLineString])

}

object MosaicMultiLineStringOGC extends GeometryReader {

    override def fromInternal(row: InternalRow): MosaicGeometry = {
        val internalGeom = InternalGeometry(row)
        val polygon = createPolyline(internalGeom.boundaries)
        val ogcMultiLineString = new OGCMultiLineString(polygon, MosaicGeometryOGC.spatialReference)
        MosaicMultiLineStringOGC(ogcMultiLineString)
    }

    def createPolyline(shellCollection: Array[Array[InternalCoord]], dontClose: Boolean = false): Polyline = {
        // noinspection ZeroIndexToHead
        def addPath(polyline: Polyline, path: Array[InternalCoord]): Unit = {
            if (path.nonEmpty) {
                val start = path.head
                val end = path.last

                val tail =
                    if (dontClose && start.equals(end)) {
                        path.tail.dropRight(1)
                    } else {
                        path.tail
                    }

                polyline.startPath(start.coords(0), start.coords(1))
                for (point <- tail) polyline.lineTo(point.coords(0), point.coords(1))
            }
        }

        val polyline = new Polyline()
        for (shell <- shellCollection) addPath(polyline, shell)

        polyline
    }

    def apply(geometry: OGCGeometry): MosaicMultiLineStringOGC = {
        new MosaicMultiLineStringOGC(geometry.asInstanceOf[OGCMultiLineString])
    }

    override def fromPoints(points: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value = MULTILINESTRING): MosaicGeometry = {
        throw new UnsupportedOperationException("fromPoints is not intended for creating MultiLineStrings")
    }

    override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryOGC.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryOGC.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryOGC.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometry = MosaicGeometryOGC.fromHEX(hex)

    override def fromKryo(row: InternalRow): MosaicGeometry = MosaicGeometryOGC.fromKryo(row)

}
