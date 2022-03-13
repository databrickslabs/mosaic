package com.databricks.mosaic.core.geometry

import com.databricks.mosaic.core.geometry.point.MosaicPoint

trait MosaicGeometry extends GeometryWriter with Serializable {

    def numPoints: Int

    def translate(xd: Double, yd: Double): MosaicGeometry

    def scale(xd: Double, yd: Double): MosaicGeometry

    def rotate(td: Double): MosaicGeometry

    def getLength: Double

    def distance(geom2: MosaicGeometry): Double

    def isValid: Boolean

    def getGeometryType: String

    def getArea: Double

    def getAPI: String

    def getCentroid: MosaicPoint

    def isEmpty: Boolean

    def getBoundary: Seq[MosaicPoint]

    def getHoles: Seq[Seq[MosaicPoint]]

    def boundary: MosaicGeometry

    def buffer(distance: Double): MosaicGeometry

    def simplify(tolerance: Double): MosaicGeometry

    def intersection(other: MosaicGeometry): MosaicGeometry

    def intersects(other: MosaicGeometry): Boolean

    def union(other: MosaicGeometry): MosaicGeometry

    def contains(other: MosaicGeometry): Boolean

    def flatten: Seq[MosaicGeometry]

    def equals(other: MosaicGeometry): Boolean

    def equals(other: java.lang.Object): Boolean

    def hashCode: Int

    def convexHull: MosaicGeometry

    def minMaxCoord(dimension: String, func: String): Double = {
        val coordArray = this.getBoundary
        val unitArray = dimension match {
            case "X" => coordArray.map(_.getX)
            case "Y" => coordArray.map(_.getY)
            case "Z" => coordArray.map(_.getZ)
        }
        func match {
            case "MIN" => unitArray.min
            case "MAX" => unitArray.max
        }
    }

}
