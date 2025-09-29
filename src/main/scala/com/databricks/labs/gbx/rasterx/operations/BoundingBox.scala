package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.GDAL
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.gdal.gdal.Dataset
import org.gdal.osr.{CoordinateTransformation, SpatialReference}
import org.locationtech.jts.algorithm.Orientation
import org.locationtech.jts.geom.{Coordinate, Geometry}

object BoundingBox {

    def bbox(ds: Dataset, destSR: SpatialReference): Geometry = {
        val xSize = ds.GetRasterXSize
        val ySize = ds.GetRasterYSize
        windowBBox((0, 0, xSize, ySize), ds, destSR)
    }

    def windowBBox(window: (Int, Int, Int, Int), ds: Dataset, destSR: SpatialReference): Geometry = {
        val gt = ds.GetGeoTransform
        val srcSR = ds.GetSpatialRef

        val tf = new CoordinateTransformation(srcSR, destSR)

        val p1 = tf.TransformPoint(gt(0) + window._1 * gt(1), gt(3) + window._2 * gt(5))
        val p2 = tf.TransformPoint(gt(0) + window._3 * gt(1), gt(3) + window._2 * gt(5))
        val p3 = tf.TransformPoint(gt(0) + window._3 * gt(1), gt(3) + window._4 * gt(5))
        val p4 = tf.TransformPoint(gt(0) + window._1 * gt(1), gt(3) + window._4 * gt(5))
        val p5 = p1 // Closing the polygon

        val points = Array(p1, p2, p3, p4, p5)
        val isCCW = Orientation.isCCW(points.map(p => new Coordinate(p(0), p(1))))
        val pointsOrd = if (isCCW) points else points.reverse
        val bbox = JTS.polygonFromXYs(pointsOrd.map(p => (p(0), p(1))))

        val dstEPSGCode = SpatialRefOps.getEPSGCode(destSR)
        bbox.setSRID(dstEPSGCode)

        if (destSR.IsSame(GDAL.WSG84) == 1) {
            // Make sure is safe wrt AntiMeridian
            bbox // TODO: Handle AntiMeridian crossing if necessary
        } else {
            bbox
        }
    }

}
