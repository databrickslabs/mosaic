package com.databricks.labs.mosaic.core.geometry.multilinestring

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineStringJTS
import com.databricks.labs.mosaic.core.geometry.point.MosaicPointJTS
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.{LINESTRING, MULTILINESTRING}
import com.databricks.labs.mosaic.core.types.model._
import org.locationtech.jts.geom._

class MosaicMultiLineStringJTS(multiLineString: MultiLineString) extends MosaicGeometryJTS(multiLineString) with MosaicMultiLineString {

    override def getBoundary: MosaicGeometryJTS = {
        val shellGeom = multiLineString.getBoundary
        shellGeom.setSRID(multiLineString.getSRID)
        MosaicGeometryJTS(shellGeom)
    }

    override def getShells: Seq[MosaicLineStringJTS] =
        for (i <- 0 until multiLineString.getNumGeometries) yield MosaicLineStringJTS(multiLineString.getGeometryN(i))

    override def mapXY(f: (Double, Double) => (Double, Double)): MosaicMultiLineStringJTS = {
        MosaicMultiLineStringJTS.fromSeq(asSeq.map(_.mapXY(f)))
    }

    override def asSeq: Seq[MosaicLineStringJTS] =
        for (i <- 0 until multiLineString.getNumGeometries) yield {
            val geom = multiLineString.getGeometryN(i).asInstanceOf[LineString]
            geom.setSRID(multiLineString.getSRID)
            new MosaicLineStringJTS(geom)
        }

    override def getHolePoints: Seq[Seq[Seq[MosaicPointJTS]]] = Nil

    override def getShellPoints: Seq[Seq[MosaicPointJTS]] = getShells.map(_.asSeq)

    override def getHoles: Seq[Seq[MosaicLineStringJTS]] = Nil

    override def flatten: Seq[MosaicGeometryJTS] = asSeq

}

object MosaicMultiLineStringJTS extends GeometryReader {

    override def fromSeq[T <: MosaicGeometry](
        geomSeq: Seq[T],
        geomType: GeometryTypeEnum.Value = MULTILINESTRING
    ): MosaicMultiLineStringJTS = {
        val gf = new GeometryFactory()

        if (geomSeq.isEmpty) {
            // For empty sequence return an empty geometry with default Spatial Reference
            return MosaicMultiLineStringJTS(gf.createMultiLineString())
        }
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

    override def fromWKB(wkb: Array[Byte]): MosaicGeometryJTS = MosaicGeometryJTS.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometryJTS = MosaicGeometryJTS.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometryJTS = MosaicGeometryJTS.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometryJTS = MosaicGeometryJTS.fromHEX(hex)

}
