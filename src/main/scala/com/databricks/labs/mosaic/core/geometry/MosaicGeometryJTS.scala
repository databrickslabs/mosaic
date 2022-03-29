package com.databricks.labs.mosaic.core.geometry

import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineStringJTS
import com.databricks.labs.mosaic.core.geometry.multilinestring.MosaicMultiLineStringJTS
import com.databricks.labs.mosaic.core.geometry.multipoint.MosaicMultiPointJTS
import com.databricks.labs.mosaic.core.geometry.multipolygon.MosaicMultiPolygonJTS
import com.databricks.labs.mosaic.core.geometry.point.{MosaicPoint, MosaicPointJTS}
import com.databricks.labs.mosaic.core.geometry.polygon.MosaicPolygonJTS
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum._
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import org.apache.commons.io.output.ByteArrayOutputStream
import org.locationtech.jts.geom.{Geometry, GeometryCollection}
import org.locationtech.jts.geom.util.AffineTransformation
import org.locationtech.jts.io._
import org.locationtech.jts.io.geojson.{GeoJsonReader, GeoJsonWriter}
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier

import org.apache.spark.sql.catalyst.InternalRow

abstract class MosaicGeometryJTS(geom: Geometry) extends MosaicGeometry {

    override def getNumGeometries: Int = geom.getNumGeometries

    override def reduceFromMulti: MosaicGeometry = {
        val n = geom.getNumGeometries
        if (n == 1) {
            MosaicGeometryJTS(geom.getGeometryN(0))
        } else {
            this
        }
    }

    override def translate(xd: Double, yd: Double): MosaicGeometryJTS = {
        val transformation = AffineTransformation.translationInstance(xd, yd)
        MosaicGeometryJTS(transformation.transform(geom))
    }

    override def scale(xd: Double, yd: Double): MosaicGeometryJTS = {
        val transformation = AffineTransformation.scaleInstance(xd, yd)
        MosaicGeometryJTS(transformation.transform(geom))
    }

    override def rotate(td: Double): MosaicGeometryJTS = {
        val transformation = AffineTransformation.rotationInstance(td)
        MosaicGeometryJTS(transformation.transform(geom))
    }

    override def getAPI: String = "JTS"

    override def getCentroid: MosaicPointJTS = new MosaicPointJTS(geom.getCentroid)

    override def isEmpty: Boolean = geom.isEmpty

    override def boundary: MosaicGeometryJTS = MosaicGeometryJTS(geom.getBoundary)

    override def buffer(distance: Double): MosaicGeometryJTS = MosaicGeometryJTS(geom.buffer(distance))

    override def simplify(tolerance: Double = 1e-8): MosaicGeometryJTS =
        MosaicGeometryJTS(
          DouglasPeuckerSimplifier.simplify(geom, tolerance)
        )

    override def intersection(other: MosaicGeometry): MosaicGeometryJTS = {
        val otherGeom = other.asInstanceOf[MosaicGeometryJTS].getGeom
        val intersection = this.geom.intersection(otherGeom)
        MosaicGeometryJTS(intersection)
    }

    override def intersects(other: MosaicGeometry): Boolean = {
        val otherGeom = other.asInstanceOf[MosaicGeometryJTS].getGeom
        this.geom.intersects(otherGeom)
    }

    def getGeom: Geometry = geom

    override def union(other: MosaicGeometry): MosaicGeometry = {
        val otherGeom = other.asInstanceOf[MosaicGeometryJTS].getGeom
        val union = this.geom.union(otherGeom)
        MosaicGeometryJTS(union)
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

    override def convexHull: MosaicGeometryJTS = MosaicGeometryJTS(geom.convexHull())

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

    override def numPoints: Int = geom.getNumPoints

    override def getSpatialReference: Int = geom.getSRID

    override def setSpatialReference(srid: Int): Unit = geom.setSRID(srid)

}

object MosaicGeometryJTS extends GeometryReader {

    @transient val kryo = new Kryo()
    kryo.register(classOf[MosaicGeometryJTS])

    override def fromWKT(wkt: String): MosaicGeometryJTS = MosaicGeometryJTS(new WKTReader().read(wkt))

    override def fromHEX(hex: String): MosaicGeometryJTS = {
        val bytes = WKBReader.hexToBytes(hex)
        fromWKB(bytes)
    }

    override def fromWKB(wkb: Array[Byte]): MosaicGeometryJTS = MosaicGeometryJTS(new WKBReader().read(wkb))

    override def fromJSON(geoJson: String): MosaicGeometryJTS = MosaicGeometryJTS(new GeoJsonReader().read(geoJson))

    def apply(geom: Geometry): MosaicGeometryJTS = {
        geom.setSRID(4326)
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
    }

    override def fromPoints(points: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value): MosaicGeometryJTS = {
        reader(geomType.id).fromPoints(points, geomType).asInstanceOf[MosaicGeometryJTS]
    }

    override def fromInternal(row: InternalRow): MosaicGeometryJTS = {
        val typeId = row.getInt(0)
        reader(typeId).fromInternal(row).asInstanceOf[MosaicGeometryJTS]
    }

    override def fromKryo(row: InternalRow): MosaicGeometryJTS = {
        val typeId = row.getInt(0)
        reader(typeId).fromKryo(row).asInstanceOf[MosaicGeometryJTS]
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
