package com.databricks.labs.mosaic.core.geometry

import com.databricks.labs.mosaic.core.geometry.api.{GeometryAPI, JTS}
import com.databricks.labs.mosaic.core.geometry.geometrycollection.MosaicGeometryCollectionJTS
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
import org.locationtech.jts.algorithm.hull.ConcaveHull
import org.locationtech.jts.geom.{Geometry, GeometryCollection, GeometryFactory}
import org.locationtech.jts.geom.util.AffineTransformation
import org.locationtech.jts.io._
import org.locationtech.jts.io.geojson.{GeoJsonReader, GeoJsonWriter}
import org.locationtech.jts.operation.buffer.{BufferOp, BufferParameters}
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier

import java.util

abstract class MosaicGeometryJTS(geom: Geometry) extends MosaicGeometry {

    override def getNumGeometries: Int = geom.getNumGeometries

    override def getDimension: Int = geom.getDimension

    def compactGeometry: MosaicGeometryJTS = {
        val geometries = for (i <- 0 until getNumGeometries) yield geom.getGeometryN(i)
        val result = MosaicGeometryJTS.compactCollection(geometries, getSpatialReference)
        result.setSRID(geom.getSRID)
        MosaicGeometryJTS(result)
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

    override def getCentroid: MosaicPointJTS = {
        val centroid = geom.getCentroid
        centroid.setSRID(geom.getSRID)
        MosaicPointJTS(centroid)
    }

    override def getAnyPoint: MosaicPointJTS = {
        // while this doesn't return the centroid but an arbitrary point via getCoordinate in JTS, 
        // inlike getCentroid this supports a Z coordinate.

        val coord = geom.getCoordinate
        val gf = new GeometryFactory()
        val point = gf.createPoint(coord)
        MosaicPointJTS(point)
    }

    override def isEmpty: Boolean = geom.isEmpty

    override def boundary: MosaicGeometryJTS = MosaicGeometryJTS(geom.getBoundary)

    override def envelope: MosaicGeometryJTS = MosaicGeometryJTS(geom.getEnvelope)

    override def buffer(distance: Double): MosaicGeometryJTS = {
        buffer(distance, "")
    }

    override def buffer(distance: Double, bufferStyleParameters: String): MosaicGeometryJTS = {

        val gBuf = new BufferOp(geom)

        if (bufferStyleParameters contains "=") {
            val params = bufferStyleParameters
                .split(" ")
                .map(_.split("="))
                .map { case Array(k, v) => (k, v) }
                .toMap

            if (params.contains("endcap")) {
                val capStyle = params.getOrElse("endcap", "")
                val capStyleConst = capStyle match {
                    case "round"  => BufferParameters.CAP_ROUND
                    case "flat"   => BufferParameters.CAP_FLAT
                    case "square" => BufferParameters.CAP_SQUARE
                    case _        => BufferParameters.CAP_ROUND
                }
                gBuf.setEndCapStyle(capStyleConst)
            }
            if (params.contains("quad_segs")) {
                val quadSegs = params.getOrElse("quad_segs", "8")
                gBuf.setQuadrantSegments(quadSegs.toInt)
            }
        }
        val buffered = gBuf.getResultGeometry(distance)
        buffered.setSRID(geom.getSRID)
        MosaicGeometryJTS(buffered)
    }

    override def bufferCapStyle(distance: Double, capStyle: String): MosaicGeometryJTS = {
        val capStyleConst = capStyle match {
            case "round"  => BufferParameters.CAP_ROUND
            case "flat"   => BufferParameters.CAP_FLAT
            case "square" => BufferParameters.CAP_SQUARE
            case _        => BufferParameters.CAP_ROUND
        }
        val gBuf = new BufferOp(geom)
        gBuf.setEndCapStyle(capStyleConst)
        val buffered = gBuf.getResultGeometry(distance)
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
        if (intersection.getNumGeometries > 1) {
            val geometries = for (i <- 0 until intersection.getNumGeometries) yield intersection.getGeometryN(i)
            val result = MosaicGeometryJTS.compactCollection(geometries, getSpatialReference)
            result.setSRID(geom.getSRID)
            MosaicGeometryJTS(result)
        } else {
            intersection.setSRID(geom.getSRID)
            MosaicGeometryJTS(intersection)
        }
    }

    override def intersects(other: MosaicGeometry): Boolean = {
        val otherGeom = other.asInstanceOf[MosaicGeometryJTS].getGeom
        this.geom.intersects(otherGeom)
    }

    override def difference(other: MosaicGeometry): MosaicGeometryJTS = {
        val otherGeom = other.asInstanceOf[MosaicGeometryJTS].getGeom
        val leftType = GeometryTypeEnum.fromString(getGeometryType)
        val rightType = GeometryTypeEnum.fromString(other.getGeometryType)
        // Difference not supported for GeometryCollection in base APIs for JTS
        // If either of the geometries is a GeometryCollection, we need to
        // handle it differently, the logic is in the MosaicGeometryCollectionJTS
        val difference =
            if (leftType == GEOMETRYCOLLECTION) {
                this.asInstanceOf[MosaicGeometryCollectionJTS].difference(other)
            } else if (rightType == GEOMETRYCOLLECTION) {
                other.asInstanceOf[MosaicGeometryCollectionJTS].difference(this)
            } else {
                MosaicGeometryJTS(this.geom.difference(otherGeom))
            }
        difference.setSpatialReference(getSpatialReference)
        difference
    }

    override def union(other: MosaicGeometry): MosaicGeometryJTS = {
        val otherGeom = other.asInstanceOf[MosaicGeometryJTS].getGeom
        val leftType = GeometryTypeEnum.fromString(getGeometryType)
        val rightType = GeometryTypeEnum.fromString(other.getGeometryType)
        // Union not supported for GeometryCollection in base APIs for JTS
        // If either of the geometries is a GeometryCollection, we need to
        // handle it differently, the logic is in the MosaicGeometryCollectionJTS
        val union =
            if (leftType == GEOMETRYCOLLECTION) {
                this.asInstanceOf[MosaicGeometryCollectionJTS].union(other)
            } else if (rightType == GEOMETRYCOLLECTION) {
                other.asInstanceOf[MosaicGeometryCollectionJTS].union(this)
            } else {
                MosaicGeometryJTS(this.geom.union(otherGeom))
            }
        union.setSpatialReference(this.getSpatialReference)
        union
    }

    override def contains(geom2: MosaicGeometry): Boolean = geom.contains(geom2.asInstanceOf[MosaicGeometryJTS].getGeom)

    override def within(geom2: MosaicGeometry): Boolean = geom.within(geom2.asInstanceOf[MosaicGeometryJTS].getGeom)

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
        val otherGeom = other.asInstanceOf[MosaicGeometryJTS].getGeom
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

    override def concaveHull(lengthRatio: Double, allow_holes: Boolean = false): MosaicGeometryJTS = {
        val concaveHull = ConcaveHull.concaveHullByLengthRatio(geom, lengthRatio, allow_holes)
        concaveHull.setSRID(geom.getSRID)
        MosaicGeometryJTS(concaveHull)
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

    override def transformCRSXY(sridTo: Int): MosaicGeometryJTS = super.transformCRSXY(sridTo, None).asInstanceOf[MosaicGeometryJTS]

    override def getAPI: GeometryAPI = JTS
}

object MosaicGeometryJTS extends GeometryReader {

    @transient val kryo = new Kryo()
    kryo.register(classOf[MosaicGeometryJTS])

    override def fromWKT(wkt: String): MosaicGeometryJTS = MosaicGeometryJTS(new WKTReader().read(wkt))

    // noinspection DuplicatedCode
    def compactCollection(geometries: Seq[Geometry], srid: Int): Geometry = {
        def appendGeometries(geometries: util.ArrayList[Geometry], toAppend: Seq[Geometry]): Unit = {
            if (toAppend.length == 1 && !toAppend.head.isEmpty) {
                geometries.add(toAppend.head)
            } else if (toAppend.length > 1) {
                val compacted = toAppend.reduce(_ union _)
                if (!compacted.isEmpty) {
                    geometries.add(compacted)
                }
            }
        }

        val withType = geometries.map(g => GeometryTypeEnum.fromString(g.getGeometryType) -> g).flatMap {
            case (GEOMETRYCOLLECTION, g) =>
                val collection = g.asInstanceOf[GeometryCollection]
                for (i <- 0 until collection.getNumGeometries)
                    yield GeometryTypeEnum.fromString(collection.getGeometryN(i).getGeometryType) -> collection.getGeometryN(i)
            case (gType, g)              => Seq(gType -> g)
        }
        val points = withType.filter(g => g._1 == POINT || g._1 == MULTIPOINT).map(_._2)
        val polygons = withType.filter(g => g._1 == POLYGON || g._1 == MULTIPOLYGON).map(_._2)
        val lines = withType.filter(g => g._1 == LINESTRING || g._1 == MULTILINESTRING).map(_._2)
        val geomArray = new util.ArrayList[Geometry]()

        appendGeometries(geomArray, points)
        appendGeometries(geomArray, lines)
        appendGeometries(geomArray, polygons)

        val gf = new GeometryFactory()
        val geom = gf.buildGeometry(geomArray)
        val result =
            if (geom.getNumGeometries == 1) {
                geom.getGeometryN(0)
            } else {
                geom
            }
        result.setSRID(srid)
        result
    }

    def apply(geom: Geometry): MosaicGeometryJTS = {
        GeometryTypeEnum.fromString(geom.getGeometryType) match {
            case POINT              => MosaicPointJTS(geom)
            case MULTIPOINT         => MosaicMultiPointJTS(geom)
            case POLYGON            => MosaicPolygonJTS(geom)
            case MULTIPOLYGON       => MosaicMultiPolygonJTS(geom)
            case LINESTRING         => MosaicLineStringJTS(geom)
            case MULTILINESTRING    => MosaicMultiLineStringJTS(geom)
            case LINEARRING         => MosaicLineStringJTS(geom)
            case GEOMETRYCOLLECTION => MosaicGeometryCollectionJTS(geom)
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
            case POINT              => MosaicPointJTS
            case MULTIPOINT         => MosaicMultiPointJTS
            case POLYGON            => MosaicPolygonJTS
            case MULTIPOLYGON       => MosaicMultiPolygonJTS
            case LINESTRING         => MosaicLineStringJTS
            case MULTILINESTRING    => MosaicMultiLineStringJTS
            case GEOMETRYCOLLECTION => MosaicGeometryCollectionJTS
        }

}
