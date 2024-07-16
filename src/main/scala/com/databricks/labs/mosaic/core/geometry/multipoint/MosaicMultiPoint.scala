package com.databricks.labs.mosaic.core.geometry.multipoint

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineString
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint
import com.databricks.labs.mosaic.core.MosaicCoreException
import com.databricks.labs.mosaic.core.geometry.multilinestring.MosaicMultiLineString
import com.databricks.labs.mosaic.core.geometry.polygon.MosaicPolygon

trait MosaicMultiPoint extends MosaicGeometry {

    def asSeq: Seq[MosaicPoint]

    override def getHoles: Seq[Seq[MosaicLineString]]

    override def flatten: Seq[MosaicGeometry]

    override def getHolePoints: Seq[Seq[Seq[MosaicPoint]]]

    override def getShellPoints: Seq[Seq[MosaicPoint]]

    override def getShells: Seq[MosaicLineString] =
        throw MosaicCoreException.InvalidGeometryOperation("getShells should not be called on MultiPoints.")

    def triangulate(breaklines: Seq[MosaicLineString], tol: Double): Seq[MosaicPolygon]

    def interpolateElevation(breaklines: Seq[MosaicLineString], gridPoints: MosaicMultiPoint, tolerance: Double) : MosaicMultiPoint

    def meshGrid(origin: MosaicPoint, xCells: Int, yCells: Int, xSize: Double, ySize: Double): MosaicMultiPoint

}
