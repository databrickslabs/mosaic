package com.databricks.labs.mosaic.core.geometry.multipoint

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.linestring.{MosaicLineString, MosaicLineStringJTS}
import com.databricks.labs.mosaic.core.geometry.multilinestring.MosaicMultiLineStringJTS
import com.databricks.labs.mosaic.core.geometry.point.{MosaicPoint, MosaicPointJTS}
import com.databricks.labs.mosaic.core.geometry.polygon.{MosaicPolygon, MosaicPolygonJTS}
import com.databricks.labs.mosaic.core.geometry.triangulation.JTSConformingDelaunayTriangulationBuilder
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.{MULTIPOINT, POINT}
import com.databricks.labs.mosaic.core.types.model._
import org.apache.spark.sql.catalyst.InternalRow
import org.locationtech.jts.geom._
import org.locationtech.jts.geom.util.{LinearComponentExtracter, PolygonExtracter}
import org.locationtech.jts.index.strtree.STRtree
import org.locationtech.jts.linearref.LengthIndexedLine

import scala.collection.JavaConverters._

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

    override def triangulate(breaklines: Seq[MosaicLineString], mergeTolerance: Double, snapTolerance: Double, splitPointFinder: TriangulationSplitPointTypeEnum.Value): Seq[MosaicPolygon] = {

        val triangulator = JTSConformingDelaunayTriangulationBuilder(multiPoint)
        val geomFact = multiPoint.getFactory
        if (breaklines.nonEmpty) {
            triangulator.setConstraints(MosaicMultiLineStringJTS.fromSeq(breaklines).getGeom)
        }

        triangulator.setTolerance(mergeTolerance)

        val trianglesGeomCollection = triangulator.getTriangles
        val trianglePolygons = PolygonExtracter.getPolygons(trianglesGeomCollection).asScala.map(_.asInstanceOf[Polygon])

        val postProcessedTrianglePolygons = postProcessTriangulation(trianglePolygons, MosaicMultiLineStringJTS.fromSeq(breaklines).getGeom, snapTolerance)
        postProcessedTrianglePolygons.map(MosaicPolygonJTS(_))
    }

    /** Update Z values of the triangle vertices that have NaN Z values by interpolating from the constraint lines
     *
     * @param trianglePolygons: Sequence of triangles, output from the triangulation method
     * @param constraintLineGeom: Geometry containing the constraint lines
     * @param tolerance: Tolerance value for the triangulation, used to buffer points and match to constraint lines
     * @return Sequence of triangles with updated Z values
     * */
    private def postProcessTriangulation(trianglePolygons: Seq[Polygon], constraintLineGeom: Geometry, tolerance: Double): Seq[Polygon] = {
        val geomFact = constraintLineGeom.getFactory

        val constraintLines =
            LinearComponentExtracter.getLines(constraintLineGeom)
                .iterator().asScala.toSeq
                .map(_.asInstanceOf[LineString])

        val constraintLinesTree = new STRtree(4)
        constraintLines.foreach(l => constraintLinesTree.insert(l.getEnvelopeInternal, l))

        trianglePolygons.map(
            t => {
                val coords = t.getCoordinates.map(
                    c => {
                        /*
                        * overwrite the z values for every coordinate lying
                        * within a fraction of the value of `tolerance`.
                        */
                        val coordPoint = geomFact.createPoint(c)
                        val originatingLineString = constraintLinesTree.query(new Envelope(c))
                            .iterator().asScala.toSeq
                            .map(_.asInstanceOf[LineString])
                            .find(l => l.intersects(coordPoint.buffer(tolerance)))
                        originatingLineString match {
                            case Some(l) =>
                                val indexedLine = new LengthIndexedLine(l)
                                val index = indexedLine.indexOf(c)
                                indexedLine.extractPoint(index)
                            case None => c
                        }
                    }
                )
                geomFact.createPolygon(coords)
            }
        )
    }

    override def interpolateElevation(
                                         breaklines: Seq[MosaicLineString], gridPoints: MosaicMultiPoint,
                                         mergeTolerance: Double, snapTolerance: Double,
                                         splitPointFinder: TriangulationSplitPointTypeEnum.Value): MosaicMultiPointJTS = {
        val triangles = triangulate(breaklines, mergeTolerance, snapTolerance, splitPointFinder)
            .asInstanceOf[Seq[MosaicPolygonJTS]]

        val tree = new STRtree(4)
        triangles.foreach(p => tree.insert(p.getGeom.getEnvelopeInternal, p.getGeom))

        val result = gridPoints.asSeq
            .map(_.asInstanceOf[MosaicPointJTS])
            .map(p => {
                val point = p.getGeom.asInstanceOf[Point]
                point -> tree.query(p.getGeom.getEnvelopeInternal).asScala
                    .map(_.asInstanceOf[Polygon])
                    .find(_.intersects(point))
            }).toMap
            .collect({ case (pt, Some(ply)) => pt -> ply })
            .map({
                case (point: Point, poly: Polygon) =>
                    val polyCoords = poly.getCoordinates
                    val tri = new Triangle(polyCoords(0), polyCoords(1), polyCoords(2))
                    val z = tri.interpolateZ(point.getCoordinate)
                    if (z.isNaN) { throw new Exception("Interpolated Z value is NaN") }
                    val interpolatedPoint = MosaicPointJTS(point.getFactory.createPoint(new Coordinate(point.getX, point.getY, z)))
                    interpolatedPoint.setSpatialReference(getSpatialReference)
                    interpolatedPoint
            }).toSeq
        MosaicMultiPointJTS.fromSeq(result)
    }

    override def pointGrid(origin: MosaicPoint, xCells: Int, yCells: Int, xSize: Double, ySize: Double): MosaicMultiPointJTS = {
        val gridPoints = for (i <- 0 until xCells; j <- 0 until yCells) yield {
            val x = origin.getX + i * xSize + xSize / 2
            val y = origin.getY + j * ySize + ySize / 2
            val gridPoint = MosaicPointJTS(multiPoint.getFactory.createPoint(new Coordinate(x, y)))
            gridPoint.setSpatialReference(getSpatialReference)
            gridPoint
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
