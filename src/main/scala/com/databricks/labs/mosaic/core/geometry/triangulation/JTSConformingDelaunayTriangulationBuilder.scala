package com.databricks.labs.mosaic.core.geometry.triangulation

import com.databricks.labs.mosaic.core.types.model.TriangulationSplitPointTypeEnum
import org.locationtech.jts.geom.{Coordinate, CoordinateList, Envelope, Geometry, LineString, MultiPoint}
import org.locationtech.jts.geom.util.LinearComponentExtracter
import org.locationtech.jts.triangulate.{ConformingDelaunayTriangulator, ConstraintSplitPointFinder, ConstraintVertex, DelaunayTriangulationBuilder, MidpointSplitPointFinder, NonEncroachingSplitPointFinder, Segment}
import org.locationtech.jts.triangulate.quadedge.QuadEdgeSubdivision

import java.util

class JTSConformingDelaunayTriangulationBuilder(geom: MultiPoint) {

        var tolerance: Double = 0.0
        val constraintVertexMap = new util.HashMap[Coordinate, ConstraintVertex]
        var constraintLines: Geometry = null
        var splitPointFinder: ConstraintSplitPointFinder = null

        def siteCoords: CoordinateList = DelaunayTriangulationBuilder.extractUniqueCoordinates(geom)

        def siteEnv: Envelope = DelaunayTriangulationBuilder.envelope(siteCoords)

        private def createSiteVertices(coords: CoordinateList): util.ArrayList[ConstraintVertex] =
        {
            val vertices = new util.ArrayList[ConstraintVertex]
            coords.toCoordinateArray
                .filter(coord => !constraintVertexMap.containsKey(coord))
                .map(new ConstraintVertex(_))
                .foreach(v => {
                    vertices.add(v)
                })
            vertices
        }

        def setTolerance(tolerance: Double): Unit = {
            this.tolerance = tolerance
        }

        def setConstraints(constraintLines: Geometry): Unit = {
            this.constraintLines = constraintLines
        }

        def setSplitPointFinder(splitPointFinder: TriangulationSplitPointTypeEnum.Value): Unit = {
            this.splitPointFinder =
                splitPointFinder match {
                    case TriangulationSplitPointTypeEnum.MIDPOINT =>
                        new MidpointSplitPointFinder
                    case TriangulationSplitPointTypeEnum.NONENCROACHING =>
                        new NonEncroachingSplitPointFinder
                }
        }

        def createVertices(geom: Geometry): Unit = {
            geom.getCoordinates.foreach(coord => {
                val v = new ConstraintVertex(coord)
                constraintVertexMap.put(coord, v)
            })
        }

        def createConstraintSegments(geometry: Geometry): util.ArrayList[Segment] = {
            val constraintSegs = new util.ArrayList[Segment]
            LinearComponentExtracter.getLines(geometry)
                .toArray
                .map(_.asInstanceOf[LineString])
                .filter(!_.isEmpty)
                .foreach(l => createConstraintSegments(l, constraintSegs))
            constraintSegs
        }

        def createConstraintSegments(line: LineString, constraintSegs: util.ArrayList[Segment]): Unit = {
            val coords = line.getCoordinates
            coords.zip(coords.tail)
                .map(c => new Segment(c._1, c._2))
                .foreach(constraintSegs.add)
        }

        def create(): QuadEdgeSubdivision = {
            var segments: util.ArrayList[Segment] = null
            if (constraintLines != null) {
                siteEnv.expandToInclude(constraintLines.getEnvelopeInternal())
                createVertices(constraintLines)
                segments = createConstraintSegments(constraintLines)
            }
            val sites = createSiteVertices(siteCoords)
            val cdt = new ConformingDelaunayTriangulator(sites, tolerance)

            cdt.setConstraints(segments, new util.ArrayList(constraintVertexMap.values()))
            cdt.formInitialDelaunay()
            if (constraintLines != null) { cdt.enforceConstraints() }
            cdt.getSubdivision()
        }

        def getTriangles: Geometry = {
            val subdiv = create()
            subdiv.getTriangles(geom.getFactory)
        }

}

object JTSConformingDelaunayTriangulationBuilder {
    def apply(geom: MultiPoint): JTSConformingDelaunayTriangulationBuilder = new JTSConformingDelaunayTriangulationBuilder(geom)
}
