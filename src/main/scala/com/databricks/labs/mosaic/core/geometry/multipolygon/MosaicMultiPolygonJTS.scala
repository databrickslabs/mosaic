package com.databricks.labs.mosaic.core.geometry.multipolygon

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineString
import com.databricks.labs.mosaic.core.geometry.polygon.MosaicPolygonJTS
import com.databricks.labs.mosaic.core.types.model.{GeometryTypeEnum, InternalGeometry}
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.{MULTIPOLYGON, POLYGON}
import org.apache.spark.sql.catalyst.InternalRow
import org.locationtech.jts.geom._

class MosaicMultiPolygonJTS(multiPolygon: MultiPolygon) extends MosaicGeometryJTS(multiPolygon) with MosaicMultiPolygon {

    override def toInternal: InternalGeometry = {
        val n = multiPolygon.getNumGeometries
        val polygons = for (i <- 0 until n) yield MosaicPolygonJTS(multiPolygon.getGeometryN(i)).toInternal
        val boundaries = polygons.map(_.boundaries.head).toArray
        val holes = polygons.flatMap(_.holes).toArray
        new InternalGeometry(MULTIPOLYGON.id, getSpatialReference, boundaries, holes)
    }

    override def getBoundary: MosaicGeometry = {
        val geom = multiPolygon.getBoundary
        geom.setSRID(multiPolygon.getSRID)
        MosaicGeometryJTS(geom)
    }

    override def getShells: Seq[MosaicLineString] = {
        val n = multiPolygon.getNumGeometries
        val shells = for (i <- 0 until n) yield {
            val polygon = MosaicPolygonJTS(multiPolygon.getGeometryN(i).asInstanceOf[Polygon])
            polygon.getShells
        }
        shells.flatten
    }

    override def getHoles: Seq[Seq[MosaicLineString]] = {
        val n = multiPolygon.getNumGeometries
        val holes = for (i <- 0 until n) yield {
            val polygon = MosaicPolygonJTS(multiPolygon.getGeometryN(i).asInstanceOf[Polygon])
            polygon.getHoles
        }
        holes.flatten
    }

    override def mapXY(f: (Double, Double) => (Double, Double)): MosaicGeometry = {
        MosaicMultiPolygonJTS.fromSeq(
          asSeq.map(_.asInstanceOf[MosaicPolygonJTS].mapXY(f).asInstanceOf[MosaicPolygonJTS])
        )
    }

    override def asSeq: Seq[MosaicGeometry] =
        for (i <- 0 until multiPolygon.getNumGeometries) yield {
            val geom = multiPolygon.getGeometryN(i)
            geom.setSRID(multiPolygon.getSRID)
            MosaicGeometryJTS(geom)
        }

}

object MosaicMultiPolygonJTS extends GeometryReader {

    override def fromInternal(row: InternalRow): MosaicGeometry = {
        val gf = new GeometryFactory()
        val internalGeom = InternalGeometry(row)

        gf.createLinearRing(gf.createLineString().getCoordinates)
        val polygons = internalGeom.boundaries.zip(internalGeom.holes).map { case (boundaryRing, holesRings) =>
            val shell = gf.createLinearRing(boundaryRing.map(_.toCoordinate))
            val holes = holesRings.map(ring => ring.map(_.toCoordinate)).map(gf.createLinearRing)
            gf.createPolygon(shell, holes)
        }
        val multiPolygon = gf.createMultiPolygon(polygons)
        multiPolygon.setSRID(internalGeom.srid)
        MosaicMultiPolygonJTS(multiPolygon)
    }

    override def fromSeq[T <: MosaicGeometry](geomSeq: Seq[T], geomType: GeometryTypeEnum.Value = MULTIPOLYGON): MosaicMultiPolygonJTS = {

        val gf = new GeometryFactory()

        if (geomSeq.isEmpty) {
            // For empty sequence return an empty geometry with default Spatial Reference
            return MosaicMultiPolygonJTS(gf.createMultiPolygon())
        }

        val spatialReference = geomSeq.head.getSpatialReference
        val newGeom = GeometryTypeEnum.fromString(geomSeq.head.getGeometryType) match {
            case POLYGON                       =>
                val extractedPolys = geomSeq.map(_.asInstanceOf[MosaicPolygonJTS])
                gf.createMultiPolygon(extractedPolys.map(_.getGeom.asInstanceOf[Polygon]).toArray)
            case other: GeometryTypeEnum.Value => throw new UnsupportedOperationException(
                  s"MosaicGeometry.fromSeq() cannot create ${geomType.toString} from ${other.toString} geometries."
                )
        }
        newGeom.setSRID(spatialReference)
        MosaicMultiPolygonJTS(newGeom)
    }

    def apply(multiPolygon: Geometry): MosaicMultiPolygonJTS = new MosaicMultiPolygonJTS(multiPolygon.asInstanceOf[MultiPolygon])

    override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryJTS.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryJTS.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryJTS.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometry = MosaicGeometryJTS.fromHEX(hex)

}
