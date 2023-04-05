package com.databricks.labs.mosaic.core.geometry.linestring

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.multilinestring.MosaicMultiLineStringESRI
import com.databricks.labs.mosaic.core.geometry.point.MosaicPointESRI
import com.databricks.labs.mosaic.core.types.model._
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.{LINESTRING, POINT}
import com.esri.core.geometry.{Polyline, SpatialReference}
import com.esri.core.geometry.ogc.{OGCGeometry, OGCLineString}
import org.apache.spark.sql.catalyst.InternalRow

class MosaicLineStringESRI(lineString: OGCLineString) extends MosaicGeometryESRI(lineString) with MosaicLineString {

    def this() = this(null)

    override def getShellPoints: Seq[Seq[MosaicPointESRI]] = Seq(MosaicLineStringESRI.getPoints(lineString))

    override def toInternal: InternalGeometry = {
        val shell = for (i <- 0 until lineString.numPoints()) yield {
            val point = lineString.pointN(i)
            InternalCoord(MosaicPointESRI(point).coord)
        }
        new InternalGeometry(LINESTRING.id, getSpatialReference, Array(shell.toArray), Array(Array(Array())))
    }

    override def getBoundary: MosaicGeometryESRI = MosaicGeometryESRI(lineString.boundary())

    override def getLength: Double = lineString.length()

    override def numPoints: Int = lineString.numPoints()

    override def mapXY(f: (Double, Double) => (Double, Double)): MosaicLineStringESRI =
        MosaicLineStringESRI.fromSeq(asSeq.map(_.mapXY(f).asInstanceOf[MosaicPointESRI]))

    override def asSeq: Seq[MosaicPointESRI] = getShellPoints.head

    override def getHoles: Seq[Seq[MosaicLineStringESRI]] = Nil

    override def getShells: Seq[MosaicLineStringESRI] = Seq(this)

    override def flatten: Seq[MosaicGeometryESRI] = Seq(this)

    override def getHolePoints: Seq[Seq[Seq[MosaicPointESRI]]] = Nil

}

object MosaicLineStringESRI extends GeometryReader {

    def getPoints(lineString: OGCLineString): Seq[MosaicPointESRI] = {
        for (i <- 0 until lineString.numPoints()) yield MosaicPointESRI(lineString.pointN(i))
    }

    override def fromInternal(row: InternalRow): MosaicLineStringESRI = {
        val internalGeom = InternalGeometry(row)
        val polyline = MosaicMultiLineStringESRI.createPolyline(internalGeom.boundaries)
        val spatialReference = MosaicGeometryESRI.getSRID(internalGeom.srid)
        val ogcLineString = new OGCLineString(polyline, 0, spatialReference)
        MosaicLineStringESRI(ogcLineString)
    }

    override def fromSeq[T <: MosaicGeometry](geomSeq: Seq[T], geomType: GeometryTypeEnum.Value = LINESTRING): MosaicLineStringESRI = {
        if (geomSeq.isEmpty) {
            // For empty sequence return an empty geometry with default Spatial Reference
            return MosaicLineStringESRI(new OGCLineString(new Polyline(), 0, MosaicGeometryESRI.defaultSpatialReference))
        }
        val spatialReference = SpatialReference.create(geomSeq.head.getSpatialReference)
        val newGeom = GeometryTypeEnum.fromString(geomSeq.head.getGeometryType) match {
            case POINT                                                =>
                val extractedPoints = geomSeq.map(_.asInstanceOf[MosaicPointESRI])
                new OGCLineString(
                  MosaicMultiLineStringESRI.createPolyline(
                    Array(extractedPoints.map(c => InternalCoord(Seq(c.coord.getX, c.coord.getY))).toArray),
                    dontClose = true
                  ),
                  0,
                  spatialReference
                )
            case other: GeometryTypeEnum.Value if other == LINESTRING =>
                throw new Error(
                  s"Joining a sequence of ${other.toString} to create a ${geomType.toString} geometry is not yet supported"
                )
            case other: GeometryTypeEnum.Value                        => throw new UnsupportedOperationException(
                  s"MosaicGeometry.fromSeq() cannot create ${geomType.toString} from ${other.toString} geometries."
                )
        }
        MosaicLineStringESRI(newGeom)
    }

    def apply(ogcGeometry: OGCGeometry): MosaicLineStringESRI = {
        new MosaicLineStringESRI(ogcGeometry.asInstanceOf[OGCLineString])
    }

    override def fromWKB(wkb: Array[Byte]): MosaicGeometryESRI = MosaicGeometryESRI.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometryESRI = MosaicGeometryESRI.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometryESRI = MosaicGeometryESRI.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometryESRI = MosaicGeometryESRI.fromHEX(hex)

}
