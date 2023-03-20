package com.databricks.labs.mosaic.core.geometry

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
            case POINT           => 1
            case MULTIPOINT      => geom.asInstanceOf[OGCMultiPoint].numGeometries()
            case LINESTRING      => 1
            case MULTILINESTRING => geom.asInstanceOf[OGCMultiLineString].numGeometries()
            case POLYGON         => 1
            case MULTIPOLYGON    => geom.asInstanceOf[OGCMultiPolygon].numGeometries()
        }

    // noinspection DuplicatedCode
    override def translate(xd: Double, yd: Double): MosaicGeometryESRI = {
        val tr = new Transformation2D
        tr.setShift(xd, yd)
        val esriGeom = geom.getEsriGeometry
        esriGeom.applyTransformation(tr)
        MosaicGeometryESRI(OGCGeometry.createFromEsriGeometry(esriGeom, geom.getEsriSpatialReference))
    }

    override def reduceFromMulti: MosaicGeometryESRI = MosaicGeometryESRI(geom.reduceFromMulti())

    // noinspection DuplicatedCode
    override def scale(xd: Double, yd: Double): MosaicGeometryESRI = {
        val tr = new Transformation2D
        tr.setScale(xd, yd)
        val esriGeom = geom.getEsriGeometry
        esriGeom.applyTransformation(tr)
        MosaicGeometryESRI(OGCGeometry.createFromEsriGeometry(esriGeom, geom.getEsriSpatialReference))
    }

    // noinspection DuplicatedCode
    override def rotate(td: Double): MosaicGeometryESRI = {
        val tr = new Transformation2D
        tr.setRotate(td)
        val esriGeom = geom.getEsriGeometry
        esriGeom.applyTransformation(tr)
        MosaicGeometryESRI(OGCGeometry.createFromEsriGeometry(esriGeom, geom.getEsriSpatialReference))
    }

    override def getAPI: String = "OGC"

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
        MosaicGeometryESRI(difference)
    }

    override def union(other: MosaicGeometry): MosaicGeometryESRI = {
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
        val op: OperatorIntersection =
            OperatorFactoryLocal.getInstance.getOperator(Operator.Type.Intersection).asInstanceOf[OperatorIntersection]
        val cursor: GeometryCursor =
            op.execute(geom.getEsriGeometryCursor, another.getEsriGeometryCursor, geom.getEsriSpatialReference, null, -1)
        OGCGeometry.createFromEsriCursor(cursor, geom.getEsriSpatialReference, true)
    }

}

object MosaicGeometryESRI extends GeometryReader {

    val defaultSpatialReference: SpatialReference = SpatialReference.create(defaultSpatialReferenceId)

    @transient val kryo = new Kryo()
    kryo.register(classOf[Array[Byte]])

    override def fromWKT(wkt: String): MosaicGeometryESRI = MosaicGeometryESRI(OGCGeometry.fromText(wkt))

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
                    case None                                                                                           =>
                        val emptyPolygon = MosaicPolygonESRI.fromWKT("POLYGON EMPTY")
                        emptyPolygon.setSpatialReference(geom.getEsriSpatialReference.getID)
                        emptyPolygon
                }
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
            case POINT           => MosaicPointESRI
            case MULTIPOINT      => MosaicMultiPointESRI
            case POLYGON         => MosaicPolygonESRI
            case MULTIPOLYGON    => MosaicMultiPolygonESRI
            case LINESTRING      => MosaicLineStringESRI
            case MULTILINESTRING => MosaicMultiLineStringESRI
        }

    override def fromInternal(row: InternalRow): MosaicGeometryESRI = {
        val typeId = row.getInt(0)
        reader(typeId).fromInternal(row).asInstanceOf[MosaicGeometryESRI]
    }

}
