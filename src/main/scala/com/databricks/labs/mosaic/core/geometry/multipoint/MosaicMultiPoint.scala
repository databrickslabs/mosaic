package com.databricks.labs.mosaic.core.geometry.multipoint

import com.databricks.labs.mosaic.core.MosaicCoreException
import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineString
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint
import com.databricks.labs.mosaic.core.geometry.polygon.MosaicPolygon
import com.databricks.labs.mosaic.core.types.model.TriangulationSplitPointTypeEnum

trait MosaicMultiPoint extends MosaicGeometry {

    def asSeq: Seq[MosaicPoint]

    override def getHoles: Seq[Seq[MosaicLineString]]

    override def flatten: Seq[MosaicGeometry]

    override def getHolePoints: Seq[Seq[Seq[MosaicPoint]]]

    override def getShellPoints: Seq[Seq[MosaicPoint]]

    override def getShells: Seq[MosaicLineString] =
        throw MosaicCoreException.InvalidGeometryOperation("getShells should not be called on MultiPoints.")

    /**
     * Triangulates this MultiPoint geometry with optional breaklines.
     *
     * @param breaklines The breaklines to use for the triangulation.
     * @param mergeTolerance The tolerance to use to simplify the triangulation by merging nearby points.
     * @param snapTolerance The tolerance to use for post-processing the results of the triangulation (snapping
     *                      newly created points against their originating breaklines.
     * @return A sequence of MosaicPolygon geometries.
     */
    def triangulate(breaklines: Seq[MosaicLineString], mergeTolerance: Double, snapTolerance: Double, splitPointFinder: TriangulationSplitPointTypeEnum.Value): Seq[MosaicPolygon]

    /**
     * Interpolates the elevation of the grid points using the breaklines.
     *
     * @param breaklines The breaklines to use for the interpolation.
     * @param gridPoints The grid points to interpolate the elevation for.
     * @param mergeTolerance The tolerance to use to simplify the triangulation by merging nearby points.
     * @param snapTolerance The tolerance to use for post-processing the results of the triangulation (snapping
     *                      newly created points against their originating breaklines.
     * @param splitPointFinder The method to use to find split points to include `breaklines` in the triangulation.
     * @return A MosaicMultiPoint geometry with the interpolated elevation.
     */
    def interpolateElevation(breaklines: Seq[MosaicLineString], gridPoints: MosaicMultiPoint, mergeTolerance: Double, snapTolerance: Double, splitPointFinder: TriangulationSplitPointTypeEnum.Value) : MosaicMultiPoint

    /**
     * Creates a regular point grid from the origin point with the specified number of cells and cell sizes.
     *
     * @param origin The origin point of the mesh grid.
     * @param xCells The number of cells in the x direction.
     * @param yCells The number of cells in the y direction.
     * @param xSize The size of the cells in the x direction.
     * @param ySize The size of the cells in the y direction.
     * @return A MosaicMultiPoint geometry representing the grid.
     */
    def pointGrid(origin: MosaicPoint, xCells: Int, yCells: Int, xSize: Double, ySize: Double): MosaicMultiPoint

}
