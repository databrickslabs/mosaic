package com.databricks.labs.mosaic.core.geometry.multilinestring

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.linestring.{MosaicLineString, MosaicLineStringESRI}
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint
import com.databricks.labs.mosaic.core.types.model._
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.MULTILINESTRING
import com.esri.core.geometry.{Polyline, SpatialReference}
import com.esri.core.geometry.ogc._

import org.apache.spark.sql.catalyst.InternalRow

class MosaicMultiLineStringESRI(multiLineString: OGCMultiLineString)
    extends MosaicGeometryESRI(multiLineString)
      with MosaicMultiLineString {

    override def toInternal: InternalGeometry = {
        val shells = for (i <- 0 until multiLineString.numGeometries()) yield {
            val lineString = multiLineString.geometryN(i).asInstanceOf[OGCLineString]
            MosaicLineStringESRI(lineString).toInternal.boundaries.head
        }
        new InternalGeometry(MULTILINESTRING.id, getSpatialReference, shells.toArray, Array(Array(Array())))
    }

    override def getLength: Double = multiLineString.length()

    override def getBoundary: MosaicGeometry = MosaicGeometryESRI(multiLineString.boundary())

    override def getShells: Seq[MosaicLineString] =
        for (i <- 0 until multiLineString.numGeometries()) yield MosaicLineStringESRI(multiLineString.geometryN(i))

    override def asSeq: Seq[MosaicLineString] =
        for (i <- 0 until multiLineString.numGeometries())
            yield new MosaicLineStringESRI(multiLineString.geometryN(i).asInstanceOf[OGCLineString])

    override def numPoints: Int =
        (for (i <- 0 until multiLineString.numGeometries()) yield multiLineString.geometryN(i).asInstanceOf[OGCLineString].numPoints()).sum

    override def mapXY(f: (Double, Double) => (Double, Double)): MosaicGeometry = {
        MosaicMultiLineStringESRI.fromLines(asSeq.map(_.mapXY(f).asInstanceOf[MosaicLineStringESRI]))
    }

}

object MosaicMultiLineStringESRI extends GeometryReader {

    override def fromInternal(row: InternalRow): MosaicGeometry = {
        val internalGeom = InternalGeometry(row)
        val polygon = createPolyline(internalGeom.boundaries)
        val spatialReference =
            if (internalGeom.srid != 0) {
                SpatialReference.create(internalGeom.srid)
            } else {
                MosaicGeometryESRI.defaultSpatialReference
            }
        val ogcMultiLineString = new OGCMultiLineString(polygon, spatialReference)
        MosaicMultiLineStringESRI(ogcMultiLineString)
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

    def apply(geometry: OGCGeometry): MosaicMultiLineStringESRI = {
        new MosaicMultiLineStringESRI(geometry.asInstanceOf[OGCMultiLineString])
    }

    override def fromPoints(points: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value = MULTILINESTRING): MosaicGeometry = {
        throw new UnsupportedOperationException("fromPoints is not intended for creating MultiLineStrings")
    }

    override def fromLines(lines: Seq[MosaicLineString], geomType: GeometryTypeEnum.Value = MULTILINESTRING): MosaicGeometry = {
        require(geomType == MULTILINESTRING)
        val sr = SpatialReference.create(lines.head.getSpatialReference)
        val polyline = new Polyline
        lines.foreach(l => polyline.add(l.asInstanceOf[MosaicLineStringESRI].getGeom.getEsriGeometry.asInstanceOf[Polyline], true))
        val geom = new OGCMultiLineString(polyline, sr)
        MosaicMultiLineStringESRI(geom)
    }

    override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryESRI.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryESRI.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryESRI.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometry = MosaicGeometryESRI.fromHEX(hex)

    override def fromKryo(row: InternalRow): MosaicGeometry = MosaicGeometryESRI.fromKryo(row)

}
