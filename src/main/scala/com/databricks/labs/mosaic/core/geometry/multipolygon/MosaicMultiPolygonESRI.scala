package com.databricks.labs.mosaic.core.geometry.multipolygon

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineStringESRI
import com.databricks.labs.mosaic.core.geometry.multilinestring.MosaicMultiLineStringESRI
import com.databricks.labs.mosaic.core.geometry.point.MosaicPointESRI
import com.databricks.labs.mosaic.core.geometry.polygon.MosaicPolygonESRI
import com.databricks.labs.mosaic.core.types.model._
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

    override def getBoundary: MosaicGeometryESRI = MosaicGeometryESRI(multiPolygon.boundary())

    override def getLength: Double = MosaicGeometryESRI(multiPolygon.boundary()).getLength

    override def numPoints: Int = {
        getHolePoints.map(_.map(_.length).sum).sum + getShellPoints.map(_.length).sum
    }

    override def getHoles: Seq[Seq[MosaicLineStringESRI]] = {
        val n = multiPolygon.numGeometries()
        val holes = for (i <- 0 until n) yield MosaicPolygonESRI(multiPolygon.geometryN(i)).getHoles
        holes.flatten
    }

    override def getShells: Seq[MosaicLineStringESRI] = {
        val n = multiPolygon.numGeometries()
        val shells = for (i <- 0 until n) yield MosaicPolygonESRI(multiPolygon.geometryN(i)).getShells
        shells.flatten
    }

    override def mapXY(f: (Double, Double) => (Double, Double)): MosaicMultiPolygonESRI = {
        MosaicMultiPolygonESRI.fromSeq(
          asSeq.map(_.asInstanceOf[MosaicPolygonESRI].mapXY(f).asInstanceOf[MosaicPolygonESRI])
        )
    }

    override def asSeq: Seq[MosaicGeometryESRI] =
        for (i <- 0 until multiPolygon.numGeometries()) yield MosaicGeometryESRI(multiPolygon.geometryN(i))

    override def flatten: Seq[MosaicGeometryESRI] = asSeq

    override def getShellPoints: Seq[Seq[MosaicPointESRI]] = getShells.map(_.asSeq)

    override def getHolePoints: Seq[Seq[Seq[MosaicPointESRI]]] = getHoles.map(_.map(_.asSeq))

}

object MosaicMultiPolygonESRI extends GeometryReader {

    override def fromInternal(row: InternalRow): MosaicMultiPolygonESRI = {
        val internalGeom = InternalGeometry(row)
        val polygon = createPolygon(internalGeom.boundaries, internalGeom.holes)
        val spatialReference = MosaicGeometryESRI.getSRID(internalGeom.srid)
        val ogcMultiLineString = new OGCMultiPolygon(polygon, spatialReference)
        MosaicMultiPolygonESRI(ogcMultiLineString)
    }

    // noinspection ZeroIndexToHead
    def createPolygon(shellCollection: Array[Array[InternalCoord]], holesCollection: Array[Array[Array[InternalCoord]]]): Polygon = {
        val polygon = new Polygon()
        shellCollection.zip(holesCollection).foreach { case (boundary, holes) =>
            val boundaryPath = MosaicMultiLineStringESRI.createPolyline(Array(boundary), dontClose = true)
            val holesPath = MosaicMultiLineStringESRI.createPolyline(holes, dontClose = true)

            val tmpBoundary = new Polygon()
            tmpBoundary.addPath(boundaryPath, 0, true)
            polygon.addPath(boundaryPath, 0, tmpBoundary.calculateArea2D() >= 0)

            // Holes need to be added to path right after corresponding external boundary, otherwise they'll
            // be picked up incorrectly by teh OGCPolygon constructor
            for (j <- 0 until holesPath.getPathCount) {
                val tmpHole = new Polygon()
                tmpHole.addPath(holesPath, j, true)
                polygon.addPath(holesPath, j, tmpHole.calculateArea2D() < 0)
            }
        }
        polygon    
    }

    def apply(multiPolygon: OGCGeometry): MosaicMultiPolygonESRI = new MosaicMultiPolygonESRI(multiPolygon.asInstanceOf[OGCMultiPolygon])

    override def fromSeq[T <: MosaicGeometry](geomSeq: Seq[T], geomType: GeometryTypeEnum.Value = MULTIPOLYGON): MosaicMultiPolygonESRI = {

        if (geomSeq.isEmpty) {
            // For empty sequence return an empty geometry with default Spatial Reference
            return MosaicMultiPolygonESRI(new OGCMultiPolygon(MosaicGeometryESRI.defaultSpatialReference))
        }

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

    override def fromWKB(wkb: Array[Byte]): MosaicGeometryESRI = MosaicGeometryESRI.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometryESRI = MosaicGeometryESRI.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometryESRI = MosaicGeometryESRI.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometryESRI = MosaicGeometryESRI.fromHEX(hex)

}
