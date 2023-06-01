package com.databricks.labs.mosaic.core.geometry

import com.databricks.labs.mosaic.core.geometry.geometrycollection.MosaicGeometryCollectionESRI
import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineStringESRI
import com.databricks.labs.mosaic.core.geometry.multilinestring.MosaicMultiLineStringESRI
import com.databricks.labs.mosaic.core.geometry.multipoint.MosaicMultiPointESRI
import com.databricks.labs.mosaic.core.geometry.multipolygon.MosaicMultiPolygonESRI
import com.databricks.labs.mosaic.core.geometry.point.MosaicPointESRI
import com.databricks.labs.mosaic.core.geometry.polygon.MosaicPolygonESRI
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum._
import com.esotericsoftware.kryo.Kryo
import com.esri.core.geometry._
import com.esri.core.geometry.ogc._
import org.apache.spark.sql.catalyst.InternalRow
import org.locationtech.jts.io.{WKBReader, WKBWriter}

import java.nio.ByteBuffer

abstract class MosaicGeometryESRI(geom: OGCGeometry) extends MosaicGeometry {

    override def getNumGeometries: Int =
        GeometryTypeEnum.fromString(geom.geometryType()) match {
            case POINT              => 1
            case MULTIPOINT         => geom.asInstanceOf[OGCMultiPoint].numGeometries()
            case LINESTRING         => 1
            case MULTILINESTRING    => geom.asInstanceOf[OGCMultiLineString].numGeometries()
            case POLYGON            => 1
            case MULTIPOLYGON       => geom.asInstanceOf[OGCMultiPolygon].numGeometries()
            case GEOMETRYCOLLECTION => geom.asInstanceOf[OGCGeometryCollection].numGeometries()
        }

    def compactGeometry: MosaicGeometryESRI = {
        val geometries = GeometryTypeEnum.fromString(geom.geometryType()) match {
            case GEOMETRYCOLLECTION =>
                val casted = geom.asInstanceOf[OGCGeometryCollection]
                for (i <- 0 until casted.numGeometries()) yield casted.geometryN(i)
            case _                  => Seq(geom)
        }
        val result = MosaicGeometryESRI.compactCollection(geometries, getSpatialReference)
        MosaicGeometryESRI(result)
    }

    // noinspection DuplicatedCode
    override def translate(xd: Double, yd: Double): MosaicGeometryESRI = {
        val tr = new Transformation2D
        tr.setShift(xd, yd)
        val esriGeom = geom.getEsriGeometry
        esriGeom.applyTransformation(tr)
        MosaicGeometryESRI(OGCGeometry.createFromEsriGeometry(esriGeom, geom.getEsriSpatialReference))
    }

    // noinspection DuplicatedCode
    override def scale(xd: Double, yd: Double): MosaicGeometryESRI = {
        val tr = new Transformation2D
        tr.setScale(xd, yd)
        val esriGeom = geom.getEsriGeometry.copy()
        esriGeom.applyTransformation(tr)
        MosaicGeometryESRI(OGCGeometry.createFromEsriGeometry(esriGeom, geom.getEsriSpatialReference))
    }

    // noinspection DuplicatedCode
    override def rotate(td: Double): MosaicGeometryESRI = {
        val tr = new Transformation2D
        tr.setRotate(td)
        val esriGeom = geom.getEsriGeometry.copy()
        esriGeom.applyTransformation(tr)
        MosaicGeometryESRI(OGCGeometry.createFromEsriGeometry(esriGeom, geom.getEsriSpatialReference))
    }

    override def getCentroid: MosaicPointESRI = MosaicPointESRI(geom.centroid())

    override def isEmpty: Boolean = geom.isEmpty

    override def buffer(distance: Double): MosaicGeometryESRI = MosaicGeometryESRI(geom.buffer(distance))

    override def simplify(tolerance: Double): MosaicGeometryESRI = MosaicGeometryESRI(geom.makeSimple())

    override def envelope: MosaicGeometryESRI = MosaicGeometryESRI(geom.envelope())

    override def intersection(other: MosaicGeometry): MosaicGeometryESRI = {
        val otherGeom = other.asInstanceOf[MosaicGeometryESRI].getGeom
        MosaicGeometryESRI(intersection(otherGeom))
    }

    override def intersects(other: MosaicGeometry): Boolean = {
        val otherGeom = other.asInstanceOf[MosaicGeometryESRI].getGeom
        this.geom.intersects(otherGeom)
    }

    override def difference(other: MosaicGeometry): MosaicGeometryESRI = {
        val otherGeom = other.asInstanceOf[MosaicGeometryESRI].getGeom
        val difference = this.getGeom.difference(otherGeom)
        if (GeometryTypeEnum.fromString(difference.geometryType()) == GEOMETRYCOLLECTION) {
            MosaicGeometryESRI(difference).compactGeometry
        } else {
            MosaicGeometryESRI(difference)
        }
    }

    override def union(other: MosaicGeometry): MosaicGeometryESRI = {
        val otherGeom = other.asInstanceOf[MosaicGeometryESRI].getGeom
        val union = this.getGeom.union(otherGeom)
        if (GeometryTypeEnum.fromString(union.geometryType()) == GEOMETRYCOLLECTION) {
            MosaicGeometryESRI(union).compactGeometry
        } else {
            MosaicGeometryESRI(union)
        }
    }

    def getGeom: OGCGeometry = geom

    override def contains(geom2: MosaicGeometry): Boolean = geom.contains(geom2.asInstanceOf[MosaicGeometryESRI].getGeom)

    /**
      * The naming convention in ESRI bindings is different. isSimple actually
      * reflects validity of a geometry.
      *
      * @see
      *   [[OGCGeometry]] for isSimple documentation.
      * @return
      *   A boolean flag indicating validity.
      */
    override def isValid: Boolean = geom.isSimple

    override def getGeometryType: String = geom.geometryType()

    override def getArea: Double = geom.getEsriGeometry.calculateArea2D()

    override def equals(other: MosaicGeometry): Boolean = {
        val otherGeom = other.asInstanceOf[MosaicGeometryESRI].getGeom
        // required to use object equals to perform exact equals
        // noinspection ComparingUnrelatedTypes
        this.getGeom.equals(otherGeom.asInstanceOf[Object]) ||
        this.getGeom.Equals(otherGeom)
    }

    override def equals(other: java.lang.Object): Boolean = false

    override def equalsTopo(other: MosaicGeometry): Boolean = {
        val otherGeom = other.asInstanceOf[MosaicGeometryESRI].getGeom
        this.getGeom.Equals(otherGeom)
    }

    override def hashCode: Int = geom.hashCode()

    override def boundary: MosaicGeometryESRI = MosaicGeometryESRI(geom.boundary())

    override def distance(geom2: MosaicGeometry): Double = this.getGeom.distance(geom2.asInstanceOf[MosaicGeometryESRI].getGeom)

    override def convexHull: MosaicGeometryESRI = MosaicGeometryESRI(geom.convexHull())

    override def unaryUnion: MosaicGeometryESRI = {
        // ESRI geometry does not directly implement unary union.
        // Here the (binary) union is used, because the union of the geometry with itself is equivalent to the unary union.
        MosaicGeometryESRI(geom.union(geom))
    }

    override def toWKT: String = geom.asText()

    override def toJSON: String = geom.asGeoJson()

    override def toHEX: String = WKBWriter.toHex(geom.asBinary().array())

    override def toWKB: Array[Byte] = geom.asBinary().array()

    override def getSpatialReference: Int = if (geom.esriSR == null) 0 else geom.getEsriSpatialReference.getID

    override def setSpatialReference(srid: Int): Unit = {
        val sr = SpatialReference.create(srid)
        geom.setSpatialReference(sr)
    }

    override def transformCRSXY(sridTo: Int): MosaicGeometryESRI = transformCRSXY(sridTo, None).asInstanceOf[MosaicGeometryESRI]

    private def intersection(another: OGCGeometry): OGCGeometry = {
        val intersection = this.getGeom.intersection(another)
        if (GeometryTypeEnum.fromString(intersection.geometryType()) == GEOMETRYCOLLECTION) {
            MosaicGeometryESRI(intersection).compactGeometry.getGeom
        }
        intersection
    }

}

object MosaicGeometryESRI extends GeometryReader {

    val defaultSpatialReference: SpatialReference = SpatialReference.create(defaultSpatialReferenceId)
    @transient val kryo = new Kryo()
    kryo.register(classOf[Array[Byte]])

    def getSRID(srid: Int): SpatialReference = {
        if (srid != 0) {
            SpatialReference.create(srid)
        } else {
            MosaicGeometryESRI.defaultSpatialReference
        }
    }

    override def fromWKT(wkt: String): MosaicGeometryESRI = MosaicGeometryESRI(OGCGeometry.fromText(wkt))

    // noinspection DuplicatedCode
    def compactCollection(geometries: Seq[OGCGeometry], srid: Int): OGCGeometry = {
        val withType = geometries.map(g => GeometryTypeEnum.fromString(g.geometryType()) -> g).flatMap {
            case (GEOMETRYCOLLECTION, g) =>
                val collection = g.asInstanceOf[OGCGeometryCollection]
                for (i <- 0 until collection.numGeometries())
                    yield GeometryTypeEnum.fromString(collection.geometryN(i).geometryType()) -> collection.geometryN(i)
            case (t, g)                  => Seq(t -> g)
        }
        val points = withType.filter(g => g._1 == POINT || g._1 == MULTIPOINT).map(_._2)
        val polygons = withType.filter(g => g._1 == POLYGON || g._1 == MULTIPOLYGON).map(_._2)
        val lines = withType.filter(g => g._1 == LINESTRING || g._1 == MULTILINESTRING).map(_._2)

        val multiPoint = if (points.length == 1) Seq(points.head) else if (points.length > 1) Seq(points.reduce(_ union _)) else Nil
        val multiLine = if (lines.length == 1) Seq(lines.head) else if (lines.length > 1) Seq(lines.reduce(_ union _)) else Nil
        val multiPolygon =
            if (polygons.length == 1) Seq(polygons.head) else if (polygons.length > 1) Seq(polygons.reduce(_ union _)) else Nil

        val pieces = multiPoint ++ multiLine ++ multiPolygon
        val result = if (pieces.length == 1) {
            pieces.head
        } else {
            pieces.reduce(_ union _)
        }
        result.setSpatialReference(getSRID(srid))
        result
    }

    def apply(geom: OGCGeometry): MosaicGeometryESRI =
        GeometryTypeEnum.fromString(geom.geometryType()) match {
            case POINT              => MosaicPointESRI(geom)
            case MULTIPOINT         => MosaicMultiPointESRI(geom)
            case POLYGON            => MosaicPolygonESRI(geom)
            case MULTIPOLYGON       => MosaicMultiPolygonESRI(geom)
            case LINESTRING         => MosaicLineStringESRI(geom)
            case MULTILINESTRING    => MosaicMultiLineStringESRI(geom)
            case GEOMETRYCOLLECTION => MosaicGeometryCollectionESRI(geom)
        }

    override def fromHEX(hex: String): MosaicGeometryESRI = {
        val bytes = WKBReader.hexToBytes(hex)
        fromWKB(bytes)
    }

    override def fromWKB(wkb: Array[Byte]): MosaicGeometryESRI = MosaicGeometryESRI(OGCGeometry.fromBinary(ByteBuffer.wrap(wkb)))

    override def fromJSON(geoJson: String): MosaicGeometryESRI = MosaicGeometryESRI(OGCGeometry.fromGeoJson(geoJson))

    override def fromSeq[T <: MosaicGeometry](geomSeq: Seq[T], geomType: GeometryTypeEnum.Value): MosaicGeometryESRI = {
        reader(geomType.id).fromSeq(geomSeq, geomType).asInstanceOf[MosaicGeometryESRI]
    }

    def reader(geomTypeId: Int): GeometryReader =
        GeometryTypeEnum.fromId(geomTypeId) match {
            case POINT              => MosaicPointESRI
            case MULTIPOINT         => MosaicMultiPointESRI
            case POLYGON            => MosaicPolygonESRI
            case MULTIPOLYGON       => MosaicMultiPolygonESRI
            case LINESTRING         => MosaicLineStringESRI
            case MULTILINESTRING    => MosaicMultiLineStringESRI
            case GEOMETRYCOLLECTION => MosaicGeometryCollectionESRI

        }

    override def fromInternal(row: InternalRow): MosaicGeometryESRI = {
        val typeId = row.getInt(0)
        reader(typeId).fromInternal(row).asInstanceOf[MosaicGeometryESRI]
    }

}
