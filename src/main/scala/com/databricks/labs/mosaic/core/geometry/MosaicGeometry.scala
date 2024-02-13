package com.databricks.labs.mosaic.core.geometry

import com.databricks.labs.mosaic.core.crs.CRSBoundsProvider
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineString
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint
import org.gdal.ogr.ogr
import org.gdal.osr.SpatialReference
import org.gdal.osr.osrConstants._
import org.locationtech.proj4j._

import java.util.Locale

trait MosaicGeometry extends GeometryWriter with Serializable {

    def getNumGeometries: Int

    def getShellPoints: Seq[Seq[MosaicPoint]]

    def getHolePoints: Seq[Seq[Seq[MosaicPoint]]]

    def numPoints: Int

    def translate(xd: Double, yd: Double): MosaicGeometry

    def scale(xd: Double, yd: Double): MosaicGeometry

    def rotate(td: Double): MosaicGeometry

    def getLength: Double

    def distance(geom2: MosaicGeometry): Double

    def difference(geom: MosaicGeometry): MosaicGeometry

    def isValid: Boolean

    def getGeometryType: String

    def getArea: Double

    def getCentroid: MosaicPoint

    def getAnyPoint: MosaicPoint

    def getDimension: Int

    def isEmpty: Boolean

    def getBoundary: MosaicGeometry

    def getShells: Seq[MosaicLineString]

    def getHoles: Seq[Seq[MosaicLineString]]

    def mapXY(f: (Double, Double) => (Double, Double)): MosaicGeometry

    def boundary: MosaicGeometry

    def buffer(distance: Double): MosaicGeometry

    def bufferCapStyle(distance: Double, capStyle: String): MosaicGeometry

    def simplify(tolerance: Double): MosaicGeometry

    def intersection(other: MosaicGeometry): MosaicGeometry

    def intersects(other: MosaicGeometry): Boolean

    def envelope: MosaicGeometry

    def extent: (Double, Double, Double, Double) = {
        val env = envelope
        (
          env.minMaxCoord("X", "MIN"),
          env.minMaxCoord("Y", "MIN"),
          env.minMaxCoord("X", "MAX"),
          env.minMaxCoord("Y", "MAX")
        )
    }

    def union(other: MosaicGeometry): MosaicGeometry

    def unaryUnion: MosaicGeometry

    def contains(other: MosaicGeometry): Boolean

    def within(other: MosaicGeometry): Boolean

    def flatten: Seq[MosaicGeometry]

    def equals(other: MosaicGeometry): Boolean

    def equals(other: java.lang.Object): Boolean

    def equalsTopo(other: MosaicGeometry): Boolean

    def hashCode: Int

    def convexHull: MosaicGeometry

    // Allow holes is set to false by default to match the behavior of the POSTGIS implementation
    def concaveHull(lengthRatio: Double, allow_holes: Boolean = false): MosaicGeometry

    def minMaxCoord(dimension: String, func: String): Double = {
        val coordArray = this.getShellPoints.map(shell => {
            val unitArray = dimension.toUpperCase(Locale.ROOT) match {
                case "X" => shell.map(_.getX)
                case "Y" => shell.map(_.getY)
                case "Z" => shell.map(_.getZ)
            }
            func.toUpperCase(Locale.ROOT) match {
                case "MIN" => unitArray.min
                case "MAX" => unitArray.max
            }
        })
        func.toUpperCase(Locale.ROOT) match {
            case "MIN" => coordArray.min
            case "MAX" => coordArray.max
        }
    }

    def transformCRSXY(sridTo: Int): MosaicGeometry

    def osrTransformCRS(srcSR: SpatialReference, destSR: SpatialReference, geometryAPI: GeometryAPI): MosaicGeometry = {
        if (srcSR.IsSame(destSR) == 1) return this
        val ogcGeometry = ogr.CreateGeometryFromWkb(this.toWKB)
        ogcGeometry.AssignSpatialReference(srcSR)
        ogcGeometry.TransformTo(destSR)
        val mosaicGeometry = geometryAPI.geometry(ogcGeometry.ExportToWkb, "WKB")
        mosaicGeometry
    }

    def transformCRSXY(sridTo: Int, sridFrom: Int): MosaicGeometry = {
        transformCRSXY(sridTo, Some(sridFrom))
    }

    def transformCRSXY(sridTo: Int, sridFrom: Option[Int]): MosaicGeometry = {

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

    def getSpatialReferenceOSR: SpatialReference = {
        val srID = getSpatialReference
        if (srID == 0) {
            null
        } else {
            val geomCRS = new SpatialReference()
            geomCRS.ImportFromEPSG(srID)
            geomCRS.SetAxisMappingStrategy(OAMS_TRADITIONAL_GIS_ORDER)
            geomCRS
        }
    }

    def hasValidCoords(crsBoundsProvider: CRSBoundsProvider, crsCode: String, which: String): Boolean = {
        val crsCodeIn = crsCode.split(":")
        val crsBounds = which.toLowerCase(Locale.ROOT) match {
            case "bounds"             => crsBoundsProvider.bounds(crsCodeIn(0), crsCodeIn(1).toInt)
            case "reprojected_bounds" => crsBoundsProvider.reprojectedBounds(crsCodeIn(0), crsCodeIn(1).toInt)
            case _                    => throw new Error("Only boundary and reprojected_boundary supported for which argument.")
        }
        (Seq(getShellPoints) ++ getHolePoints).flatten.flatten.forall(point =>
            crsBounds.lowerLeft.getX <= point.getX && point.getX <= crsBounds.upperRight.getX &&
            crsBounds.lowerLeft.getY <= point.getY && point.getY <= crsBounds.upperRight.getY
        )
    }

    def getAPI: GeometryAPI

}
