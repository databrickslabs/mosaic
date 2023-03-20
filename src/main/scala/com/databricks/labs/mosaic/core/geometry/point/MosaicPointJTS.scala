package com.databricks.labs.mosaic.core.geometry.point

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineStringJTS
import com.databricks.labs.mosaic.core.types.model.{Coordinates, _}
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.POINT
import org.apache.spark.sql.catalyst.InternalRow
import org.locationtech.jts.geom._

class MosaicPointJTS(point: Point) extends MosaicGeometryJTS(point) with MosaicPoint {

    override def geoCoord: Coordinates = Coordinates(point.getY, point.getX)

    override def coord: Coordinate = new Coordinate(point.getX, point.getY)

    override def asSeq: Seq[Double] =
        if (point.getCoordinates.length == 2) {
            Seq(getX, getY)
        } else {
            Seq(getX, getY, getZ)
        }

    override def getX: Double = point.getX

    override def getY: Double = point.getY

    override def getZ: Double = point.getCoordinate.z

    override def toInternal: InternalGeometry = {
        val shell = Array(InternalCoord(point.getCoordinate))
        new InternalGeometry(POINT.id, getSpatialReference, Array(shell), Array(Array(Array())))
    }

    override def getBoundary: MosaicGeometryJTS = {
        val geom = point.getBoundary
        geom.setSRID(point.getSRID)
        MosaicGeometryJTS(geom)
    }

    override def mapXY(f: (Double, Double) => (Double, Double)): MosaicGeometryJTS = {
        val (x_, y_) = f(getX, getY)
        MosaicPointJTS(
          new Coordinate(x_, y_),
          point.getSRID
        )
    }

    override def flatten: Seq[MosaicGeometryJTS] = List(this)

    override def getShellPoints: Seq[Seq[MosaicPointJTS]] = Seq(Seq(this))

    override def getHolePoints: Seq[Seq[Seq[MosaicPointJTS]]] = Nil

    override def getHoles: Seq[Seq[MosaicLineStringJTS]] = Nil

}

object MosaicPointJTS extends GeometryReader {

    def apply(geoCoord: Coordinates): MosaicPointJTS = {
        this.apply(new Coordinate(geoCoord.lng, geoCoord.lat), defaultSpatialReferenceId)
    }

    def apply(coord: Coordinate, srid: Int): MosaicPointJTS = {
        val gf = new GeometryFactory()
        val point = gf.createPoint(coord)
        point.setSRID(srid)
        new MosaicPointJTS(point)
    }

    def apply(coords: Seq[Double]): MosaicPointJTS = {
        val gf = new GeometryFactory()
        if (coords.length == 3) {
            val point = gf.createPoint(new Coordinate(coords(0), coords(1), coords(2)))
            new MosaicPointJTS(point)
        } else {
            val point = gf.createPoint(new Coordinate(coords(0), coords(1)))
            new MosaicPointJTS(point)
        }
    }

    override def fromInternal(row: InternalRow): MosaicGeometryJTS = {
        val gf = new GeometryFactory()
        val internalGeom = InternalGeometry(row)
        val coordinate = internalGeom.boundaries.head.head
        val point = gf.createPoint(coordinate.toCoordinate)
        point.setSRID(internalGeom.srid)
        new MosaicPointJTS(point)
    }

    override def fromSeq[T <: MosaicGeometry](geomSeq: Seq[T], geomType: GeometryTypeEnum.Value = POINT): MosaicPointJTS = {
        val gf = new GeometryFactory()
        if (geomSeq.isEmpty) {
            // For empty sequence return an empty geometry with default Spatial Reference
            return MosaicPointJTS(gf.createPoint())
        }
        val spatialReference = geomSeq.head.getSpatialReference
        val newGeom = GeometryTypeEnum.fromString(geomSeq.head.getGeometryType) match {
            case POINT                         =>
                val extractedPoint = geomSeq.head.asInstanceOf[MosaicPoint]
                gf.createPoint(extractedPoint.coord)
            case other: GeometryTypeEnum.Value => throw new UnsupportedOperationException(
                  s"MosaicGeometry.fromSeq() cannot create ${geomType.toString} from ${other.toString} geometries."
                )
        }
        newGeom.setSRID(spatialReference)
        MosaicPointJTS(newGeom)
    }

    def apply(geom: Geometry): MosaicPointJTS = new MosaicPointJTS(geom.asInstanceOf[Point])

    override def fromWKB(wkb: Array[Byte]): MosaicGeometryJTS = MosaicGeometryJTS.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometryJTS = MosaicGeometryJTS.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometryJTS = MosaicGeometryJTS.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometryJTS = MosaicGeometryJTS.fromHEX(hex)

}
