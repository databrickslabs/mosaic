package com.databricks.labs.mosaic.core.geometry

import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineString
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint

import org.locationtech.proj4j.{CoordinateTransformFactory, CRSFactory, ProjCoordinate}

trait MosaicGeometry extends GeometryWriter with Serializable {

    def getNumGeometries: Int

    def getShellPoints: Seq[Seq[MosaicPoint]]

    def getHolePoints: Seq[Seq[Seq[MosaicPoint]]]

    def reduceFromMulti: MosaicGeometry

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

    def getBoundary: MosaicGeometry

    def getShells: Seq[MosaicLineString]

    def getHoles: Seq[Seq[MosaicLineString]]

    def mapXY(f: (Double, Double) => (Double, Double)): MosaicGeometry

    def boundary: MosaicGeometry

    def buffer(distance: Double): MosaicGeometry

    def simplify(tolerance: Double): MosaicGeometry

    def intersection(other: MosaicGeometry): MosaicGeometry

    def intersects(other: MosaicGeometry): Boolean

    def union(other: MosaicGeometry): MosaicGeometry

    def unaryUnion: MosaicGeometry

    def contains(other: MosaicGeometry): Boolean

    def flatten: Seq[MosaicGeometry]

    def equals(other: MosaicGeometry): Boolean

    def equals(other: java.lang.Object): Boolean

    def equalsTopo(other: MosaicGeometry): Boolean

    def hashCode: Int

    def convexHull: MosaicGeometry

    def minMaxCoord(dimension: String, func: String): Double = {
        val coordArray = this.getShellPoints.map(shell => {
            val unitArray = dimension match {
                case "X" => shell.map(_.getX)
                case "Y" => shell.map(_.getY)
                case "Z" => shell.map(_.getZ)
            }
            func match {
                case "MIN" => unitArray.min
                case "MAX" => unitArray.max
            }
        })
        func match {
            case "MIN" => coordArray.min
            case "MAX" => coordArray.max
        }
    }

    def transformCRSXY(sridTo: Int, sridFrom: Option[Int] = None): MosaicGeometry = {

        val crsFactory = new CRSFactory
        val crsFrom = crsFactory.createFromName(f"epsg:${sridFrom.getOrElse(getSpatialReference)}")
        val crsTo = crsFactory.createFromName(f"epsg:$sridTo")

        val ctFactory = new CoordinateTransformFactory
        val trans = ctFactory.createTransform(crsFrom, crsTo)

        val pIn = new ProjCoordinate
        val pOut = new ProjCoordinate

        def mapper(x: Double, y: Double): (Double, Double) = {
            pIn.setValue(x, y)
            trans.transform(pIn, pOut)
            (pOut.x, pOut.y)
        }
        val mosaicGeometry = mapXY(mapper)
        mosaicGeometry.setSpatialReference(sridTo)
        mosaicGeometry
    }

    def getSpatialReference: Int

    def setSpatialReference(srid: Int): Unit

}
