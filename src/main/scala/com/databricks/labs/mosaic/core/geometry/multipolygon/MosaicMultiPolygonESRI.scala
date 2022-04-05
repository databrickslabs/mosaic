package com.databricks.labs.mosaic.core.geometry.multipolygon

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineString
import com.databricks.labs.mosaic.core.geometry.multilinestring.MosaicMultiLineStringESRI
import com.databricks.labs.mosaic.core.geometry.point.{MosaicPoint, MosaicPointESRI}
import com.databricks.labs.mosaic.core.geometry.polygon.{MosaicPolygon, MosaicPolygonESRI}
import com.databricks.labs.mosaic.core.types.model.{GeometryTypeEnum, _}
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.{MULTIPOLYGON, POLYGON}
import com.esri.core.geometry.{Polygon, SpatialReference}
import com.esri.core.geometry.ogc.{OGCGeometry, OGCMultiPolygon}

import org.apache.spark.sql.catalyst.InternalRow

class MosaicMultiPolygonESRI(multiPolygon: OGCMultiPolygon) extends MosaicGeometryESRI(multiPolygon) with MosaicMultiPolygon {

    override def toInternal: InternalGeometry = {
        val n = multiPolygon.numGeometries()
        val polygons = for (i <- 0 until n) yield MosaicPolygonESRI(multiPolygon.geometryN(i)).toInternal
        val boundaries = polygons.map(_.boundaries.head).toArray
        val holes = polygons.flatMap(_.holes).toArray
        new InternalGeometry(MULTIPOLYGON.id, getSpatialReference, boundaries, holes)
    }

    override def getBoundary: MosaicGeometry = MosaicGeometryESRI(multiPolygon.boundary())

    override def getLength: Double = MosaicGeometryESRI(multiPolygon.boundary()).getLength

    override def asSeq: Seq[MosaicGeometry] =
        for (i <- 0 until multiPolygon.numGeometries()) yield MosaicGeometryESRI(multiPolygon.geometryN(i))

    override def numPoints: Int = {
        getHolePoints.map(_.length).sum + getShellPoints.map(_.length).sum
    }

    override def getHoles: Seq[Seq[MosaicLineString]] = {
        val n = multiPolygon.numGeometries()
        val holes = for (i <- 0 until n) yield MosaicPolygonESRI(multiPolygon.geometryN(i)).getHoles
        holes.flatten
    }

    override def getShells: Seq[MosaicLineString] = {
        val n = multiPolygon.numGeometries()
        val shells = for (i <- 0 until n) yield MosaicPolygonESRI(multiPolygon.geometryN(i)).getShells
        shells.flatten
    }

    override def mapXY(f: (Double, Double) => (Double, Double)): MosaicGeometry = {
        MosaicMultiPolygonESRI.fromSeq(
          asSeq.map(_.asInstanceOf[MosaicPolygonESRI].mapXY(f).asInstanceOf[MosaicPolygonESRI])
        )
    }

}

object MosaicMultiPolygonESRI extends GeometryReader {

    override def fromInternal(row: InternalRow): MosaicGeometry = {
        val internalGeom = InternalGeometry(row)
        val polygon = createPolygon(internalGeom.boundaries, internalGeom.holes)
        val spatialReference =
            if (internalGeom.srid != 0) {
                SpatialReference.create(internalGeom.srid)
            } else {
                MosaicGeometryESRI.defaultSpatialReference
            }
        val ogcMultiLineString = new OGCMultiPolygon(polygon, spatialReference)
        MosaicMultiPolygonESRI(ogcMultiLineString)
    }

    // noinspection ZeroIndexToHead
    def createPolygon(shellCollection: Array[Array[InternalCoord]], holesCollection: Array[Array[Array[InternalCoord]]]): Polygon = {
        val boundariesPath = MosaicMultiLineStringESRI.createPolyline(shellCollection, dontClose = true)
        val holesPathsCollection = holesCollection.map(MosaicMultiLineStringESRI.createPolyline(_, dontClose = true))

        val polygon = new Polygon()

        for (i <- 0 until boundariesPath.getPathCount) {
            val tmpPolygon = new Polygon()
            tmpPolygon.addPath(boundariesPath, i, true)
            if (tmpPolygon.calculateArea2D() < 0) {
                polygon.addPath(boundariesPath, i, false)
            } else {
                polygon.addPath(boundariesPath, i, true)
            }
        }
        holesPathsCollection.foreach(holesPath =>
            for (i <- 0 until holesPath.getPathCount) {
                val tmpPolygon = new Polygon()
                tmpPolygon.addPath(holesPath, i, true)
                if (tmpPolygon.calculateArea2D() < 0) {
                    polygon.addPath(holesPath, i, true)
                } else {
                    polygon.addPath(holesPath, i, false)
                }
            }
        )

        polygon
    }

    def apply(multiPolygon: OGCGeometry): MosaicMultiPolygonESRI = new MosaicMultiPolygonESRI(multiPolygon.asInstanceOf[OGCMultiPolygon])

    override def fromSeq[T <: MosaicGeometry](geomSeq: Seq[T], geomType: GeometryTypeEnum.Value = MULTIPOLYGON): MosaicMultiPolygonESRI = {
        val spatialReference = SpatialReference.create(geomSeq.head.getSpatialReference)
        val newGeom = GeometryTypeEnum.fromString(geomSeq.head.getGeometryType) match {
            case POLYGON                       =>
                val extractedPolys = geomSeq.map(_.toInternal)
                val newMultiPolygon = createPolygon(extractedPolys.flatMap(_.boundaries).toArray, extractedPolys.flatMap(_.holes).toArray)
                new OGCMultiPolygon(newMultiPolygon, spatialReference)
            case other: GeometryTypeEnum.Value => throw new UnsupportedOperationException(
                  s"MosaicGeometry.fromSeq() cannot create ${geomType.toString} from ${other.toString} geometries."
                )
        }
        MosaicMultiPolygonESRI(newGeom)
    }

    override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryESRI.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryESRI.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryESRI.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometry = MosaicGeometryESRI.fromHEX(hex)

    override def fromKryo(row: InternalRow): MosaicGeometry = MosaicGeometryESRI.fromKryo(row)

}
