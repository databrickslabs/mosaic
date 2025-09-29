package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.vectorx.jts.{JTS, JTSConformingDelaunayTriangulationBuilder}
import org.locationtech.jts.geom.util.{LinearComponentExtracter, PolygonExtracter}
import org.locationtech.jts.geom._
import org.locationtech.jts.index.strtree.STRtree
import org.locationtech.jts.linearref.LengthIndexedLine

import java.util.Locale
import scala.jdk.CollectionConverters._

object InterpolateElevation {

    object TriangulationSplitPointTypeEnum extends Enumeration {

        val MIDPOINT: TriangulationSplitPointTypeEnum.Value = Value("MIDPOINT")
        val NONENCROACHING: TriangulationSplitPointTypeEnum.Value = Value("NONENCROACHING")

        def fromString(value: String): TriangulationSplitPointTypeEnum.Value =
            TriangulationSplitPointTypeEnum.values
                .find(_.toString == value.toUpperCase(Locale.ROOT))
                .getOrElse(
                  throw new Error(
                    s"Invalid mode for triangulation split point type: $value." +
                        s" Must be one of ${TriangulationSplitPointTypeEnum.values.mkString(",")}"
                  )
                )

    }

    def interpolate(
        multipoint: MultiPoint,
        breaklines: Seq[LineString],
        gridPoints: MultiPoint,
        mergeTolerance: Double,
        snapTolerance: Double
    ): Seq[Point] = {
        val triangles = triangulate(multipoint, breaklines, mergeTolerance, snapTolerance)

        val tree = new STRtree(4)
        triangles.foreach(p => tree.insert(p.getEnvelopeInternal, p))

        val pointsSeq = (0 until gridPoints.getNumGeometries)
            .map(i => gridPoints.getGeometryN(i))
            .collect { case p: org.locationtech.jts.geom.Point => p }
        pointsSeq
            .map(p => {
                p -> tree
                    .query(p.getEnvelopeInternal)
                    .asScala
                    .map(_.asInstanceOf[Polygon])
                    .find(_.intersects(p))
            })
            .toMap
            .collect({ case (pt, Some(ply)) => pt -> ply })
            .map({ case (point: Point, poly: Polygon) =>
                val polyCoords = poly.getCoordinates
                val tri = new Triangle(polyCoords(0), polyCoords(1), polyCoords(2))
                val z = tri.interpolateZ(point.getCoordinate)
                if (z.isNaN) { throw new Exception("Interpolated Z value is NaN") }
                val ip = JTS.point(new Coordinate(point.getX, point.getY, z))
                ip.setSRID(multipoint.getSRID)
                ip
            })
            .toSeq
    }

    def triangulate(
        multiPoint: Geometry,
        breaklines: Seq[Geometry],
        mergeTolerance: Double,
        snapTolerance: Double
    ): Seq[Geometry] = {
        val multiLineString = JTS.multiLineString(breaklines)
        val triangulator = JTSConformingDelaunayTriangulationBuilder(multiPoint)
        if (breaklines.nonEmpty) triangulator.setConstraints(multiLineString)

        triangulator.setTolerance(mergeTolerance)

        val trianglesGeomCollection = triangulator.getTriangles
        val trianglePolygons = PolygonExtracter.getPolygons(trianglesGeomCollection).asScala.map(_.asInstanceOf[Polygon])

        val postProcessedTrianglePolygons = postProcessTriangulation(trianglePolygons.toSeq, multiLineString, snapTolerance)
        postProcessedTrianglePolygons
    }

    private def postProcessTriangulation(
        trianglePolygons: Seq[Polygon],
        constraintLineGeom: Geometry,
        tolerance: Double
    ): Seq[Polygon] = {
        val geomFact = constraintLineGeom.getFactory

        val constraintLines = LinearComponentExtracter
            .getLines(constraintLineGeom)
            .iterator()
            .asScala
            .toSeq
            .map(_.asInstanceOf[LineString])

        val constraintLinesTree = new STRtree(4)
        constraintLines.foreach(l => constraintLinesTree.insert(l.getEnvelopeInternal, l))

        trianglePolygons.map(t => {
            val coords = t.getCoordinates.map(c => {
                /*
                 * overwrite the z values for every coordinate lying
                 * within a fraction of the value of `tolerance`.
                 */
                val coordPoint = geomFact.createPoint(c)
                val originatingLineString = constraintLinesTree
                    .query(new Envelope(c))
                    .iterator()
                    .asScala
                    .toSeq
                    .map(_.asInstanceOf[LineString])
                    .find(l => l.intersects(coordPoint.buffer(tolerance)))
                originatingLineString match {
                    case Some(l) =>
                        val indexedLine = new LengthIndexedLine(l)
                        val index = indexedLine.indexOf(c)
                        indexedLine.extractPoint(index)
                    case None    => c
                }
            })
            geomFact.createPolygon(coords)
        })
    }

    def pointGrid(origin: Point, xCells: Int, yCells: Int, xSize: Double, ySize: Double): MultiPoint = {
        val gridPoints = for (i <- 0 until xCells; j <- 0 until yCells) yield {
            val x = origin.getX + i * xSize + xSize / 2
            val y = origin.getY + j * ySize + ySize / 2
            val gridPoint = JTS.point(new Coordinate(x, y))
            gridPoint.setSRID(origin.getSRID)
            gridPoint
        }
        JTS.multiPoint(gridPoints.toArray)
    }

}
