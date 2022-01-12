package com.databricks.mosaic.core.geometry

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import org.apache.commons.io.output.ByteArrayOutputStream
import org.locationtech.jts.geom.{Geometry, GeometryCollection}
import org.locationtech.jts.geom.util.AffineTransformation
import org.locationtech.jts.io._
import org.locationtech.jts.io.geojson.{GeoJsonReader, GeoJsonWriter}
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier

import org.apache.spark.sql.catalyst.InternalRow

import com.databricks.mosaic.core.geometry.linestring.MosaicLineStringJTS
import com.databricks.mosaic.core.geometry.multilinestring.MosaicMultiLineStringJTS
import com.databricks.mosaic.core.geometry.multipoint.MosaicMultiPointJTS
import com.databricks.mosaic.core.geometry.multipolygon.MosaicMultiPolygonJTS
import com.databricks.mosaic.core.geometry.point.{MosaicPoint, MosaicPointJTS}
import com.databricks.mosaic.core.geometry.polygon.MosaicPolygonJTS
import com.databricks.mosaic.core.types.model.GeometryTypeEnum
import com.databricks.mosaic.core.types.model.GeometryTypeEnum._

abstract class MosaicGeometryJTS(geom: Geometry) extends MosaicGeometry {

    override def translate(xd: Double, yd: Double): MosaicGeometry = {
        val transformation = AffineTransformation.translationInstance(xd, yd)
        MosaicGeometryJTS(transformation.transform(geom))
    }

    override def scale(xd: Double, yd: Double): MosaicGeometry = {
        val transformation = AffineTransformation.scaleInstance(xd, yd)
        MosaicGeometryJTS(transformation.transform(geom))
    }

    override def rotate(td: Double): MosaicGeometry = {
        val transformation = AffineTransformation.rotationInstance(td)
        MosaicGeometryJTS(transformation.transform(geom))
    }

    override def getAPI: String = "JTS"

    override def getCentroid: MosaicPoint = new MosaicPointJTS(geom.getCentroid)

    override def isEmpty: Boolean = geom.isEmpty

    override def boundary: MosaicGeometry = MosaicGeometryJTS(geom.getBoundary)

    override def buffer(distance: Double): MosaicGeometry = MosaicGeometryJTS(geom.buffer(distance))

    override def simplify(tolerance: Double = 1e-8): MosaicGeometry =
        MosaicGeometryJTS(
          DouglasPeuckerSimplifier.simplify(geom, tolerance)
        )

    override def intersection(other: MosaicGeometry): MosaicGeometry = {
        val otherGeom = other.asInstanceOf[MosaicGeometryJTS].getGeom
        val intersection = this.geom.intersection(otherGeom)
        MosaicGeometryJTS(intersection)
    }

    override def contains(geom2: MosaicGeometry): Boolean = geom.contains(geom2.asInstanceOf[MosaicGeometryJTS].getGeom)

    override def isValid: Boolean = geom.isValid

    override def getGeometryType: String = geom.getGeometryType

    override def getArea: Double = geom.getArea

    override def equals(other: MosaicGeometry): Boolean = {
        val otherGeom = other.asInstanceOf[MosaicGeometryJTS].getGeom
        this.geom.equalsExact(otherGeom)
    }

    override def equals(other: java.lang.Object): Boolean = false

    override def hashCode: Int = geom.hashCode()

    override def getLength: Double = geom.getLength

    override def distance(geom2: MosaicGeometry): Double = getGeom.distance(geom2.asInstanceOf[MosaicGeometryJTS].getGeom)

    def getGeom: Geometry = geom

    override def toWKT: String = new WKTWriter().write(geom)

    override def toJSON: String = new GeoJsonWriter().write(geom)

    override def toHEX: String = WKBWriter.toHex(toWKB)

    override def toWKB: Array[Byte] = new WKBWriter().write(geom)

    override def toKryo: Array[Byte] = {
        val b = new ByteArrayOutputStream()
        val output = new Output(b)
        MosaicGeometryJTS.kryo.writeObject(output, this)
        val result = output.toBytes
        output.flush()
        output.close()
        result
    }

}

object MosaicGeometryJTS extends GeometryReader {

    @transient val kryo = new Kryo()
    kryo.register(classOf[MosaicGeometryJTS])

    override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryJTS(new WKTReader().read(wkt))

    override def fromHEX(hex: String): MosaicGeometry = {
        val bytes = WKBReader.hexToBytes(hex)
        fromWKB(bytes)
    }

    override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryJTS(new WKBReader().read(wkb))

    def apply(geom: Geometry): MosaicGeometryJTS =
        GeometryTypeEnum.fromString(geom.getGeometryType) match {
            case POINT              => MosaicPointJTS(geom)
            case MULTIPOINT         => MosaicMultiPointJTS(geom)
            case POLYGON            => MosaicPolygonJTS(geom)
            case MULTIPOLYGON       => MosaicMultiPolygonJTS(geom)
            case LINESTRING         => MosaicLineStringJTS(geom)
            case MULTILINESTRING    => MosaicMultiLineStringJTS(geom)
            case LINEARRING         => MosaicLineStringJTS(geom)
            // Hotfix for intersections that generate a geometry collection
            // TODO: Decide if intersection is a generator function
            // TODO: Decide if we auto flatten geometry collections
            case GEOMETRYCOLLECTION =>
                val geomCollection = geom.asInstanceOf[GeometryCollection]
                val geometries = for (i <- 0 until geomCollection.getNumGeometries) yield geomCollection.getGeometryN(i)
                geometries.find(g => Seq(POLYGON, MULTIPOLYGON).contains(GeometryTypeEnum.fromString(g.getGeometryType))) match {
                    case Some(firstChip) if GeometryTypeEnum.fromString(firstChip.getGeometryType).id == POLYGON.id      =>
                        MosaicPolygonJTS(firstChip)
                    case Some(firstChip) if GeometryTypeEnum.fromString(firstChip.getGeometryType).id == MULTIPOLYGON.id =>
                        MosaicMultiPolygonJTS(firstChip)
                    case None => MosaicPolygonJTS.fromWKT("POLYGON EMPTY").asInstanceOf[MosaicGeometryJTS]
                }
        }

    override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryJTS(new GeoJsonReader().read(geoJson))

    override def fromPoints(points: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value): MosaicGeometry = {
        reader(geomType.id).fromPoints(points, geomType)
    }

    override def fromInternal(row: InternalRow): MosaicGeometry = {
        val typeId = row.getInt(0)
        reader(typeId).fromInternal(row)
    }

    override def fromKryo(row: InternalRow): MosaicGeometry = {
        val typeId = row.getInt(0)
        reader(typeId).fromKryo(row)
    }

    def reader(geomTypeId: Int): GeometryReader =
        GeometryTypeEnum.fromId(geomTypeId) match {
            case POINT           => MosaicPointJTS
            case MULTIPOINT      => MosaicMultiPointJTS
            case POLYGON         => MosaicPolygonJTS
            case MULTIPOLYGON    => MosaicMultiPolygonJTS
            case LINESTRING      => MosaicLineStringJTS
            case MULTILINESTRING => MosaicMultiLineStringJTS
        }

}
