package com.databricks.labs.mosaic.core.geometry.multilinestring

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.linestring.{MosaicLineString, MosaicLineStringJTS}
import com.databricks.labs.mosaic.core.types.model._
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.{LINESTRING, MULTILINESTRING}
import org.apache.spark.sql.catalyst.InternalRow
import org.locationtech.jts.geom._

class MosaicMultiLineStringJTS(multiLineString: MultiLineString) extends MosaicGeometryJTS(multiLineString) with MosaicMultiLineString {

    override def toInternal: InternalGeometry = {
        val shells = for (i <- 0 until multiLineString.getNumGeometries) yield {
            val lineString = multiLineString.getGeometryN(i).asInstanceOf[LineString]
            lineString.getCoordinates.map(InternalCoord(_))
        }
        new InternalGeometry(MULTILINESTRING.id, getSpatialReference, shells.toArray, Array(Array(Array())))
    }

    override def getBoundary: MosaicGeometry = {
        val shellGeom = multiLineString.getBoundary
        shellGeom.setSRID(multiLineString.getSRID)
        MosaicGeometryJTS(shellGeom)
    }

    override def getShells: Seq[MosaicLineString] =
        for (i <- 0 until multiLineString.getNumGeometries) yield MosaicLineStringJTS(multiLineString.getGeometryN(i))

    override def mapXY(f: (Double, Double) => (Double, Double)): MosaicGeometry = {
        MosaicMultiLineStringJTS.fromSeq(asSeq.map(_.mapXY(f).asInstanceOf[MosaicLineStringJTS]))
    }

    override def asSeq: Seq[MosaicLineString] =
        for (i <- 0 until multiLineString.getNumGeometries) yield {
            val geom = multiLineString.getGeometryN(i).asInstanceOf[LineString]
            geom.setSRID(multiLineString.getSRID)
            new MosaicLineStringJTS(geom)
        }

}

object MosaicMultiLineStringJTS extends GeometryReader {

    override def fromInternal(row: InternalRow): MosaicGeometry = {
        val internalGeom = InternalGeometry(row)
        val gf = new GeometryFactory()
        val lineStrings = for (shell <- internalGeom.boundaries) yield gf.createLineString(shell.map(_.toCoordinate))
        val geometry = gf.createMultiLineString(lineStrings)
        geometry.setSRID(internalGeom.srid)
        MosaicMultiLineStringJTS(geometry)
    }

    override def fromSeq[T <: MosaicGeometry](
        geomSeq: Seq[T],
        geomType: GeometryTypeEnum.Value = MULTILINESTRING
    ): MosaicMultiLineStringJTS = {
        val gf = new GeometryFactory()
        val spatialReference = geomSeq.head.getSpatialReference
        val newGeom = GeometryTypeEnum.fromString(geomSeq.head.getGeometryType) match {
            case LINESTRING                    =>
                val extractedLines = geomSeq.map(_.asInstanceOf[MosaicLineStringJTS])
                gf.createMultiLineString(extractedLines.map(_.getGeom.asInstanceOf[LineString]).toArray)
            // scalastyle:on throwerror
            case other: GeometryTypeEnum.Value => throw new UnsupportedOperationException(
                  s"MosaicGeometry.fromSeq() cannot create ${geomType.toString} from ${other.toString} geometries."
                )
        }
        newGeom.setSRID(spatialReference)
        MosaicMultiLineStringJTS(newGeom)
    }

    def apply(geometry: Geometry): MosaicMultiLineStringJTS = {
        new MosaicMultiLineStringJTS(geometry.asInstanceOf[MultiLineString])
    }

    override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryJTS.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryJTS.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryJTS.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometry = MosaicGeometryJTS.fromHEX(hex)

}
