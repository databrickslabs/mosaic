package com.databricks.labs.mosaic.core.geometry.linestring

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.point.{MosaicPoint, MosaicPointJTS}
import com.databricks.labs.mosaic.core.types.model.{GeometryTypeEnum, _}
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.{LINEARRING, LINESTRING, POINT}
import com.esotericsoftware.kryo.io.Input
import org.locationtech.jts.geom._

import org.apache.spark.sql.catalyst.InternalRow

class MosaicLineStringJTS(lineString: LineString) extends MosaicGeometryJTS(lineString) with MosaicLineString {

    override def getShellPoints: Seq[Seq[MosaicPoint]] = Seq(MosaicLineStringJTS.getPoints(lineString))

    override def toInternal: InternalGeometry = {
        val shell = lineString.getCoordinates.map(InternalCoord(_))
        new InternalGeometry(LINESTRING.id, getSpatialReference, Array(shell), Array(Array(Array())))
    }

    override def getBoundary: MosaicGeometry = {
        val geom = lineString.getBoundary
        geom.setSRID(lineString.getSRID)
        MosaicGeometryJTS(geom)
    }

    override def mapXY(f: (Double, Double) => (Double, Double)): MosaicGeometry = {
        MosaicLineStringJTS.fromSeq(asSeq.map(_.mapXY(f).asInstanceOf[MosaicPointJTS]))
    }

}

object MosaicLineStringJTS extends GeometryReader {

    def getPoints(lineString: LineString): Seq[MosaicPoint] = {
        for (i <- 0 until lineString.getNumPoints) yield {
            val point = lineString.getPointN(i)
            point.setSRID(lineString.getSRID)
            new MosaicPointJTS(point)
        }
    }

    override def fromInternal(row: InternalRow): MosaicGeometry = {
        val internalGeom = InternalGeometry(row)
        val gf = new GeometryFactory()
        val lineString = gf.createLineString(internalGeom.boundaries.head.map(_.toCoordinate))
        lineString.setSRID(internalGeom.srid)
        MosaicLineStringJTS(lineString)
    }

    override def fromSeq[T <: MosaicGeometry](geomSeq: Seq[T], geomType: GeometryTypeEnum.Value = LINESTRING): MosaicLineStringJTS = {
        val gf = new GeometryFactory()
        val spatialReference = geomSeq.head.getSpatialReference
        val newGeom = GeometryTypeEnum.fromString(geomSeq.head.getGeometryType) match {
            case POINT                                                =>
                val extractedPoints = geomSeq.map(_.asInstanceOf[MosaicPointJTS])
                gf.createLineString(extractedPoints.map(_.coord).toArray)
            case other: GeometryTypeEnum.Value if other == LINESTRING =>
                // scalastyle:off throwerror
                throw new NotImplementedError(
                  s"Joining a sequence of ${other.toString} to create a ${geomType.toString} geometry is not yet supported"
                )
            // scalastyle:on throwerror
            case other: GeometryTypeEnum.Value                        => throw new UnsupportedOperationException(
                  s"MosaicGeometry.fromSeq() cannot create ${geomType.toString} from ${other.toString} geometries."
                )
        }
        newGeom.setSRID(spatialReference)
        MosaicLineStringJTS(newGeom)
    }

    def apply(geometry: Geometry): MosaicLineStringJTS = {
        GeometryTypeEnum.fromString(geometry.getGeometryType) match {
            case LINESTRING => new MosaicLineStringJTS(geometry.asInstanceOf[LineString])
            case LINEARRING =>
                val newGeom = new GeometryFactory().createLineString(
                  geometry.asInstanceOf[LinearRing].getCoordinates
                )
                newGeom.setSRID(geometry.getSRID)
                new MosaicLineStringJTS(newGeom)
        }
    }

    override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryJTS.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryJTS.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryJTS.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometry = MosaicGeometryJTS.fromHEX(hex)

    override def fromKryo(row: InternalRow): MosaicGeometry = {
        val kryoBytes = row.getBinary(1)
        val input = new Input(kryoBytes)
        MosaicGeometryJTS.kryo.readObject(input, classOf[MosaicLineStringJTS])
    }

}
