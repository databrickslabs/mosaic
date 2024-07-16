package com.databricks.labs.mosaic.core.geometry.multipoint

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.linestring.{MosaicLineString, MosaicLineStringJTS}
import com.databricks.labs.mosaic.core.geometry.multilinestring.{MosaicMultiLineString, MosaicMultiLineStringJTS}
import com.databricks.labs.mosaic.core.geometry.point.{MosaicPoint, MosaicPointJTS}
import com.databricks.labs.mosaic.core.geometry.polygon.{MosaicPolygon, MosaicPolygonJTS}
import com.databricks.labs.mosaic.core.types.model._
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.{MULTIPOINT, POINT}
import org.apache.spark.sql.catalyst.InternalRow
import org.locationtech.jts.geom._
import org.locationtech.jts.index.strtree.STRtree
import org.locationtech.jts.triangulate.ConformingDelaunayTriangulationBuilder

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class MosaicMultiPointJTS(multiPoint: MultiPoint) extends MosaicGeometryJTS(multiPoint) with MosaicMultiPoint {

    // noinspection DuplicatedCode
    override def toInternal: InternalGeometry = {
        val points = asSeq.map(_.coord).map(InternalCoord(_))
        new InternalGeometry(MULTIPOINT.id, getSpatialReference, Array(points.toArray), Array(Array(Array())))
    }

    override def asSeq: Seq[MosaicPointJTS] = {
        for (i <- 0 until multiPoint.getNumPoints) yield {
            val geom = multiPoint.getGeometryN(i)
            geom.setSRID(multiPoint.getSRID)
            MosaicPointJTS(geom)
        }
    }

    override def getBoundary: MosaicGeometryJTS = {
        val boundary = multiPoint.getBoundary
        boundary.setSRID(multiPoint.getSRID)
        MosaicGeometryJTS(boundary)
    }

    override def mapXY(f: (Double, Double) => (Double, Double)): MosaicGeometryJTS = {
        MosaicMultiPointJTS.fromSeq(asSeq.map(_.mapXY(f).asInstanceOf[MosaicPointJTS]))
    }

    override def getHoles: Seq[Seq[MosaicLineStringJTS]] = Nil

    override def flatten: Seq[MosaicGeometryJTS] = asSeq

    override def getHolePoints: Seq[Seq[Seq[MosaicPointJTS]]] = Nil

    override def getShellPoints: Seq[Seq[MosaicPointJTS]] = Seq(asSeq)

    def triangulate(breaklines: Seq[MosaicLineString], tolerance: Double): Seq[MosaicPolygon] = {
        val triangulator = new ConformingDelaunayTriangulationBuilder()
        val geomFact = multiPoint.getFactory

        triangulator.setSites(multiPoint)
        if (breaklines.nonEmpty) {
            val multiLineString = MosaicMultiLineStringJTS.fromSeq(breaklines)
            triangulator.setConstraints(multiLineString.getGeom)
        }
        triangulator.setTolerance(tolerance)

        val geom = triangulator.getTriangles(geomFact)
        val result = ArrayBuffer.empty[MosaicPolygonJTS]
        for (i <- 0 until geom.getNumGeometries) {
            result += MosaicPolygonJTS(geom.getGeometryN(i))
        }
        result
    }

    override def interpolateElevation(breaklines: Seq[MosaicLineString], gridPoints: MosaicMultiPoint, tolerance: Double): MosaicMultiPointJTS = {
        val triangles = triangulate(breaklines, tolerance).asInstanceOf[Seq[MosaicPolygonJTS]]
        val tree = new STRtree(4)
        triangles.foreach(p => tree.insert(p.getGeom.getEnvelopeInternal, p.getGeom))

        val result = gridPoints.asSeq
            .map(_.asInstanceOf[MosaicPointJTS])
            .map(p => {
                val point = p.getGeom.asInstanceOf[Point]
                val poly = tree.query(p.getGeom.getEnvelopeInternal)
                val polyCoords = poly.get(0).asInstanceOf[Polygon].getCoordinates
                val tri = new Triangle(polyCoords(0), polyCoords(1), polyCoords(2))
                val z = tri.interpolateZ(point.getCoordinate)
                MosaicPointJTS(point.getFactory.createPoint(new Coordinate(point.getX, point.getY, z)))
            })
        MosaicMultiPointJTS.fromSeq(result)
    }

    override def meshGrid(origin: MosaicPoint, xCells: Int, yCells: Int, xSize: Double, ySize: Double): MosaicMultiPointJTS = {
        val gridPoints = for (i <- 0 until xCells; j <- 0 until yCells) yield {
            val x = origin.getX + i * xSize
            val y = origin.getY + j * ySize
            MosaicPointJTS(multiPoint.getFactory.createPoint(new Coordinate(x, y)))
        }
        MosaicMultiPointJTS.fromSeq(gridPoints)
    }
}

object MosaicMultiPointJTS extends GeometryReader {

    // noinspection ZeroIndexToHead
    override def fromInternal(row: InternalRow): MosaicMultiPointJTS = {
        val gf = new GeometryFactory()
        val internalGeom = InternalGeometry(row)
        require(internalGeom.typeId == MULTIPOINT.id)

        val points = internalGeom.boundaries.head.map(p => gf.createPoint(p.toCoordinate))
        val multiPoint = gf.createMultiPoint(points)
        multiPoint.setSRID(internalGeom.srid)
        new MosaicMultiPointJTS(multiPoint)
    }

    override def fromSeq[T <: MosaicGeometry](geomSeq: Seq[T], geomType: GeometryTypeEnum.Value = MULTIPOINT): MosaicMultiPointJTS = {
        val gf = new GeometryFactory()
        if (geomSeq.isEmpty) {
            // For empty sequence return an empty geometry with default Spatial Reference
            return MosaicMultiPointJTS(gf.createMultiPoint())
        }
        val spatialReference = geomSeq.head.getSpatialReference
        val newGeom = GeometryTypeEnum.fromString(geomSeq.head.getGeometryType) match {
            case POINT                         =>
                val extractedPoints = geomSeq.map(_.asInstanceOf[MosaicPointJTS])
                gf.createMultiPoint(extractedPoints.map(_.getGeom.asInstanceOf[Point]).toArray)
            case other: GeometryTypeEnum.Value => throw new UnsupportedOperationException(
                  s"MosaicGeometry.fromSeq() cannot create ${geomType.toString} from ${other.toString} geometries."
                )
        }
        newGeom.setSRID(spatialReference)
        MosaicMultiPointJTS(newGeom)
    }

    def apply(geom: Geometry): MosaicMultiPointJTS = new MosaicMultiPointJTS(geom.asInstanceOf[MultiPoint])

    override def fromWKB(wkb: Array[Byte]): MosaicGeometryJTS = MosaicGeometryJTS.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometryJTS = MosaicGeometryJTS.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometryJTS = MosaicGeometryJTS.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometryJTS = MosaicGeometryJTS.fromHEX(hex)

}
