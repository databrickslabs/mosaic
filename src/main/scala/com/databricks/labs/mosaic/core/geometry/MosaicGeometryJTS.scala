package com.databricks.labs.mosaic.core.geometry

import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineStringJTS
import com.databricks.labs.mosaic.core.geometry.multilinestring.MosaicMultiLineStringJTS
import com.databricks.labs.mosaic.core.geometry.multipoint.MosaicMultiPointJTS
import com.databricks.labs.mosaic.core.geometry.multipolygon.MosaicMultiPolygonJTS
import com.databricks.labs.mosaic.core.geometry.point.MosaicPointJTS
import com.databricks.labs.mosaic.core.geometry.polygon.MosaicPolygonJTS
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum._
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.sql.catalyst.InternalRow
import org.locationtech.jts.geom.{Geometry, GeometryCollection}
import org.locationtech.jts.geom.util.AffineTransformation
import org.locationtech.jts.io._
import org.locationtech.jts.io.geojson.{GeoJsonReader, GeoJsonWriter}
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier

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

    override def getCentroid: MosaicPointJTS = {
        val centroid = geom.getCentroid
        centroid.setSRID(geom.getSRID)
        MosaicPointJTS(centroid)
    }

    override def isEmpty: Boolean = geom.isEmpty

    override def boundary: MosaicGeometryJTS = MosaicGeometryJTS(geom.getBoundary)

    override def buffer(distance: Double): MosaicGeometryJTS = {
        val buffered = geom.buffer(distance)
        buffered.setSRID(geom.getSRID)
        MosaicGeometryJTS(buffered)
    }

    override def simplify(tolerance: Double = 1e-8): MosaicGeometryJTS = {
        val simplified = DouglasPeuckerSimplifier.simplify(geom, tolerance)
        simplified.setSRID(geom.getSRID)
        MosaicGeometryJTS(
          simplified
        )
    }

    override def intersection(other: MosaicGeometry): MosaicGeometryJTS = {
        val otherGeom = other.asInstanceOf[MosaicGeometryJTS].getGeom
        val intersection = this.geom.intersection(otherGeom)
        intersection.setSRID(geom.getSRID)
        MosaicGeometryJTS(intersection)
    }

    override def intersects(other: MosaicGeometry): Boolean = {
        val otherGeom = other.asInstanceOf[MosaicGeometryJTS].getGeom
        this.geom.intersects(otherGeom)
    }

    override def difference(other: MosaicGeometry): MosaicGeometry = {
        val otherGeom = other.asInstanceOf[MosaicGeometryJTS].getGeom
        val difference = this.geom.difference(otherGeom)
        difference.setSRID(geom.getSRID)
        MosaicGeometryJTS(difference)
    }

    override def union(other: MosaicGeometry): MosaicGeometry = {
        val otherGeom = other.asInstanceOf[MosaicGeometryJTS].getGeom
        val union = this.geom.union(otherGeom)
        union.setSRID(geom.getSRID)
        MosaicGeometryJTS(union)
    }

    override def contains(geom2: MosaicGeometry): Boolean = geom.contains(geom2.asInstanceOf[MosaicGeometryJTS].getGeom)

    def getGeom: Geometry = geom

    override def isValid: Boolean = geom.isValid

    override def getGeometryType: String = geom.getGeometryType

    override def getArea: Double = geom.getArea

    override def equals(other: MosaicGeometry): Boolean = {
        val otherGeom = other.asInstanceOf[MosaicGeometryJTS].getGeom
        this.geom.equalsExact(otherGeom)
    }

    override def equals(other: java.lang.Object): Boolean = false

    override def equalsTopo(other: MosaicGeometry): Boolean = {
        var otherGeom = other.asInstanceOf[MosaicGeometryJTS].getGeom
        this.geom.equalsTopo(otherGeom)
    }

    override def hashCode: Int = geom.hashCode()

    override def getLength: Double = geom.getLength

    override def distance(geom2: MosaicGeometry): Double = getGeom.distance(geom2.asInstanceOf[MosaicGeometryJTS].getGeom)

    override def convexHull: MosaicGeometryJTS = {
        val convexHull = geom.convexHull()
        convexHull.setSRID(geom.getSRID)
        MosaicGeometryJTS(convexHull)
    }

    override def unaryUnion: MosaicGeometryJTS = {
        val unaryUnion = geom.union()
        unaryUnion.setSRID(geom.getSRID)
        MosaicGeometryJTS(unaryUnion)
    }

    override def toWKT: String = new WKTWriter().write(geom)

    override def toJSON: String = new GeoJsonWriter().write(geom)

    override def toHEX: String = WKBWriter.toHex(toWKB)

    override def toWKB: Array[Byte] = new WKBWriter().write(geom)

    override def numPoints: Int = geom.getNumPoints

    override def getSpatialReference: Int = geom.getSRID

    override def setSpatialReference(srid: Int): Unit = geom.setSRID(srid)

}

object MosaicGeometryJTS extends GeometryReader {

    @transient val kryo = new Kryo()
    kryo.register(classOf[MosaicGeometryJTS])

    override def fromWKT(wkt: String): MosaicGeometryJTS = MosaicGeometryJTS(new WKTReader().read(wkt))

    def apply(geom: Geometry): MosaicGeometryJTS = {
//        geom.setSRID(4326)
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
                    case None                                                                                            =>
                        val emptyPolygon = MosaicPolygonJTS.fromWKT("POLYGON EMPTY").asInstanceOf[MosaicGeometryJTS]
                        emptyPolygon.setSpatialReference(geom.getSRID)
                        emptyPolygon
                }
        }
    }

    override def fromHEX(hex: String): MosaicGeometryJTS = {
        val bytes = WKBReader.hexToBytes(hex)
        fromWKB(bytes)
    }

    override def fromWKB(wkb: Array[Byte]): MosaicGeometryJTS = MosaicGeometryJTS(new WKBReader().read(wkb))

    override def fromJSON(geoJson: String): MosaicGeometryJTS = MosaicGeometryJTS(new GeoJsonReader().read(geoJson))

    override def fromSeq[T <: MosaicGeometry](geomSeq: Seq[T], geomType: GeometryTypeEnum.Value): MosaicGeometryJTS = {
        reader(geomType.id).fromSeq(geomSeq, geomType).asInstanceOf[MosaicGeometryJTS]
    }

    override def fromInternal(row: InternalRow): MosaicGeometryJTS = {
        val typeId = row.getInt(0)
        reader(typeId).fromInternal(row).asInstanceOf[MosaicGeometryJTS]
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
