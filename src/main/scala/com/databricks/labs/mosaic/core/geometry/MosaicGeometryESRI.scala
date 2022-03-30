package com.databricks.labs.mosaic.core.geometry

import java.nio.ByteBuffer

import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineStringESRI
import com.databricks.labs.mosaic.core.geometry.multilinestring.MosaicMultiLineStringESRI
import com.databricks.labs.mosaic.core.geometry.multipoint.MosaicMultiPointESRI
import com.databricks.labs.mosaic.core.geometry.multipolygon.MosaicMultiPolygonESRI
import com.databricks.labs.mosaic.core.geometry.point.{MosaicPoint, MosaicPointESRI}
import com.databricks.labs.mosaic.core.geometry.polygon.MosaicPolygonESRI
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum._
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esri.core.geometry._
import com.esri.core.geometry.ogc._
import org.apache.commons.io.output.ByteArrayOutputStream
import org.locationtech.jts.io.{WKBReader, WKBWriter}

import org.apache.spark.sql.catalyst.InternalRow

abstract class MosaicGeometryESRI(geom: OGCGeometry) extends MosaicGeometry {

    override def getNumGeometries: Int =
        GeometryTypeEnum.fromString(geom.geometryType()) match {
            case POINT           => 1
            case MULTIPOINT      => geom.asInstanceOf[OGCMultiPoint].numGeometries()
            case LINESTRING      => 1
            case MULTILINESTRING => geom.asInstanceOf[OGCMultiLineString].numGeometries()
            case POLYGON         => 1
            case MULTIPOLYGON    => geom.asInstanceOf[OGCMultiPolygon].numGeometries()
        }

    // noinspection DuplicatedCode
    override def translate(xd: Double, yd: Double): MosaicGeometry = {
        val tr = new Transformation2D
        tr.setShift(xd, yd)
        val esriGeom = geom.getEsriGeometry
        esriGeom.applyTransformation(tr)
        MosaicGeometryESRI(OGCGeometry.createFromEsriGeometry(esriGeom, geom.getEsriSpatialReference))
    }

    override def reduceFromMulti: MosaicGeometry = MosaicGeometryESRI(geom.reduceFromMulti())

    // noinspection DuplicatedCode
    override def scale(xd: Double, yd: Double): MosaicGeometry = {
        val tr = new Transformation2D
        tr.setScale(xd, yd)
        val esriGeom = geom.getEsriGeometry
        esriGeom.applyTransformation(tr)
        MosaicGeometryESRI(OGCGeometry.createFromEsriGeometry(esriGeom, geom.getEsriSpatialReference))
    }

    // noinspection DuplicatedCode
    override def rotate(td: Double): MosaicGeometry = {
        val tr = new Transformation2D
        tr.setRotate(td)
        val esriGeom = geom.getEsriGeometry
        esriGeom.applyTransformation(tr)
        MosaicGeometryESRI(OGCGeometry.createFromEsriGeometry(esriGeom, geom.getEsriSpatialReference))
    }

    override def getAPI: String = "OGC"

    override def getCentroid: MosaicPoint = MosaicPointESRI(geom.centroid())

    override def isEmpty: Boolean = geom.isEmpty

    override def buffer(distance: Double): MosaicGeometry = MosaicGeometryESRI(geom.buffer(distance))

    override def simplify(tolerance: Double): MosaicGeometry = MosaicGeometryESRI(geom.makeSimple())

    override def intersection(other: MosaicGeometry): MosaicGeometry = {
        val otherGeom = other.asInstanceOf[MosaicGeometryESRI].getGeom
        MosaicGeometryESRI(intersection(otherGeom))
    }

    private def intersection(another: OGCGeometry): OGCGeometry = {
        val op: OperatorIntersection =
            OperatorFactoryLocal.getInstance.getOperator(Operator.Type.Intersection).asInstanceOf[OperatorIntersection]
        val cursor: GeometryCursor =
            op.execute(geom.getEsriGeometryCursor, another.getEsriGeometryCursor, geom.getEsriSpatialReference, null, -1)
        OGCGeometry.createFromEsriCursor(cursor, geom.getEsriSpatialReference, true)
    }

    override def intersects(other: MosaicGeometry): Boolean = {
        val otherGeom = other.asInstanceOf[MosaicGeometryESRI].getGeom
        this.geom.intersects(otherGeom)
    }

    override def union(other: MosaicGeometry): MosaicGeometry = {
        val otherGeom = other.asInstanceOf[MosaicGeometryESRI].getGeom
        MosaicGeometryESRI(this.getGeom.union(otherGeom))
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

    override def hashCode: Int = geom.hashCode()

    override def boundary: MosaicGeometry = MosaicGeometryESRI(geom.boundary())

    override def distance(geom2: MosaicGeometry): Double = this.getGeom.distance(geom2.asInstanceOf[MosaicGeometryESRI].getGeom)

    override def convexHull: MosaicGeometryESRI = MosaicGeometryESRI(geom.convexHull())

    override def toWKT: String = geom.asText()

    override def toJSON: String = geom.asGeoJson()

    override def toHEX: String = WKBWriter.toHex(geom.asBinary().array())

    override def toKryo: Array[Byte] = {
        val b = new ByteArrayOutputStream()
        val output = new Output(b)
        MosaicGeometryESRI.kryo.writeObject(output, toWKB)
        val result = output.toBytes
        output.flush()
        output.close()
        result
    }

    override def toWKB: Array[Byte] = geom.asBinary().array()

    override def getSpatialReference: Int = geom.getEsriSpatialReference.getID

    override def setSpatialReference(srid: Int): Unit = {
        val sr = SpatialReference.create(srid)
        geom.setSpatialReference(sr)
    }

}

object MosaicGeometryESRI extends GeometryReader {

    val defaultSpatialReference: SpatialReference = SpatialReference.create(defaultSpatialReferenceId)

    @transient val kryo = new Kryo()
    kryo.register(classOf[Array[Byte]])

    override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryESRI(OGCGeometry.fromText(wkt))

    def apply(geom: OGCGeometry): MosaicGeometryESRI =
        GeometryTypeEnum.fromString(geom.geometryType()) match {
            case POINT              => MosaicPointESRI(geom)
            case MULTIPOINT         => MosaicMultiPointESRI(geom)
            case POLYGON            => MosaicPolygonESRI(geom)
            case MULTIPOLYGON       => MosaicMultiPolygonESRI(geom)
            case LINESTRING         => MosaicLineStringESRI(geom)
            case MULTILINESTRING    => MosaicMultiLineStringESRI(geom)
            // Hotfix for intersections that generate a geometry collection
            // TODO: Decide if intersection is a generator function
            // TODO: Decide if we auto flatten geometry collections
            case GEOMETRYCOLLECTION =>
                val geomCollection = geom.asInstanceOf[OGCGeometryCollection]
                val geometries = for (i <- 0 until geomCollection.numGeometries()) yield geomCollection.geometryN(i)
                geometries.find(g => Seq(POLYGON, MULTIPOLYGON).contains(GeometryTypeEnum.fromString(g.geometryType()))) match {
                    case Some(firstChip) if GeometryTypeEnum.fromString(firstChip.geometryType()).id == POLYGON.id      =>
                        MosaicPolygonESRI(firstChip)
                    case Some(firstChip) if GeometryTypeEnum.fromString(firstChip.geometryType()).id == MULTIPOLYGON.id =>
                        MosaicMultiPolygonESRI(firstChip)
                    case None => MosaicPolygonESRI.fromWKT("POLYGON EMPTY").asInstanceOf[MosaicGeometryESRI]
                }
        }

    override def fromHEX(hex: String): MosaicGeometry = {
        val bytes = WKBReader.hexToBytes(hex)
        fromWKB(bytes)
    }

    override def fromWKB(wkb: Array[Byte]): MosaicGeometryESRI = MosaicGeometryESRI(OGCGeometry.fromBinary(ByteBuffer.wrap(wkb)))

    override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryESRI(OGCGeometry.fromGeoJson(geoJson))

    override def fromPoints(points: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value): MosaicGeometry = {
        reader(geomType.id).fromPoints(points, geomType)
    }

    def reader(geomTypeId: Int): GeometryReader =
        GeometryTypeEnum.fromId(geomTypeId) match {
            case POINT           => MosaicPointESRI
            case MULTIPOINT      => MosaicMultiPointESRI
            case POLYGON         => MosaicPolygonESRI
            case MULTIPOLYGON    => MosaicMultiPolygonESRI
            case LINESTRING      => MosaicLineStringESRI
            case MULTILINESTRING => MosaicMultiLineStringESRI
        }

    override def fromInternal(row: InternalRow): MosaicGeometry = {
        val typeId = row.getInt(0)
        reader(typeId).fromInternal(row)
    }

    override def fromKryo(row: InternalRow): MosaicGeometry = {
        val kryoBytes = row.getBinary(1)
        val input = new Input(kryoBytes)
        val wkb = MosaicGeometryESRI.kryo.readObject(input, classOf[Array[Byte]])
        fromWKB(wkb)
    }

}
