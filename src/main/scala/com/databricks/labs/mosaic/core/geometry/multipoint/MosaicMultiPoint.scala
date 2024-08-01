package com.databricks.labs.mosaic.core.geometry.multipoint

import com.databricks.labs.mosaic.core.MosaicCoreException
import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineString
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint
import com.databricks.labs.mosaic.core.geometry.polygon.MosaicPolygon

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
     * @param tol The tolerance to use for the triangulation.
     * @return A sequence of MosaicPolygon geometries.
     */
    def triangulate(breaklines: Seq[MosaicLineString], tol: Double): Seq[MosaicPolygon]

    /**
     * Interpolates the elevation of the grid points using the breaklines.
     *
     * @param breaklines The breaklines to use for the interpolation.
     * @param gridPoints The grid points to interpolate the elevation for.
     * @param tolerance The tolerance to use for the interpolation.
     * @return A MosaicMultiPoint geometry with the interpolated elevation.
     */
    def interpolateElevation(breaklines: Seq[MosaicLineString], gridPoints: MosaicMultiPoint, tolerance: Double) : MosaicMultiPoint

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
