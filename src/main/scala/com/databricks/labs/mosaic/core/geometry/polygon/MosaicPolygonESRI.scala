package com.databricks.labs.mosaic.core.geometry.polygon

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineStringESRI
import com.databricks.labs.mosaic.core.geometry.multipolygon.MosaicMultiPolygonESRI
import com.databricks.labs.mosaic.core.geometry.point.MosaicPointESRI
import com.databricks.labs.mosaic.core.types.model._
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum._
import com.esri.core.geometry.{Polygon, SpatialReference}
import com.esri.core.geometry.ogc._
import org.apache.spark.sql.catalyst.InternalRow

class MosaicPolygonESRI(polygon: OGCPolygon) extends MosaicGeometryESRI(polygon) with MosaicPolygon {

    def this() = this(null)

    override def toInternal: InternalGeometry = {
        def ringToInternalCoords(ring: OGCLineString): Array[InternalCoord] = {
            for (i <- 0 until ring.numPoints()) yield InternalCoord(MosaicPointESRI(ring.pointN(i)).coord)
        }.toArray

        val boundary = polygon.boundary().geometryN(0).asInstanceOf[OGCLineString]
        val shell = ringToInternalCoords(boundary)
        val holes = for (i <- 0 until polygon.numInteriorRing()) yield ringToInternalCoords(polygon.interiorRingN(i))

        new InternalGeometry(POLYGON.id, getSpatialReference, Array(shell), Array(holes.toArray))
    }

    override def getBoundary: MosaicGeometryESRI = MosaicGeometryESRI(polygon.boundary())

    override def getLength: Double = MosaicGeometryESRI(polygon.boundary()).getLength

    override def numPoints: Int = {
        getHolePoints.map(_.map(_.length).sum).sum + getShellPoints.map(_.length).sum
    }

    override def getShells: Seq[MosaicLineStringESRI] = Seq(MosaicLineStringESRI(polygon.exteriorRing()))

    override def getHoles: Seq[Seq[MosaicLineStringESRI]] =
        Seq(
          for (i <- 0 until polygon.numInteriorRing()) yield MosaicLineStringESRI(polygon.interiorRingN(i))
        )

    override def mapXY(f: (Double, Double) => (Double, Double)): MosaicGeometry = {
        val shellTransformed = getShells.head.mapXY(f)
        val holesTransformed = getHoles.head.map(_.mapXY(f))
        val newGeom = MosaicPolygonESRI.fromSeq(Seq(shellTransformed) ++ holesTransformed)
        newGeom.setSpatialReference(getSpatialReference)
        newGeom
    }

    override def asSeq: Seq[MosaicLineStringESRI] = getShells ++ getHoles.flatten

    override def getHolePoints: Seq[Seq[Seq[MosaicPointESRI]]] = getHoles.map(_.map(_.asSeq))

    override def flatten: Seq[MosaicGeometryESRI] = List(this)

    override def getShellPoints: Seq[Seq[MosaicPointESRI]] = getShells.map(_.asSeq)

}

object MosaicPolygonESRI extends GeometryReader {

    def apply(ogcGeometry: OGCGeometry): MosaicPolygonESRI = {
        new MosaicPolygonESRI(ogcGeometry.asInstanceOf[OGCPolygon])
    }

    def getPoints(lineString: OGCLineString): Seq[MosaicPointESRI] = {
        for (i <- 0 until lineString.numPoints()) yield MosaicPointESRI(lineString.pointN(i))
    }

    override def fromInternal(row: InternalRow): MosaicPolygonESRI = {
        val internalGeom = InternalGeometry(row)
        val polygon = MosaicMultiPolygonESRI.createPolygon(internalGeom.boundaries, internalGeom.holes)
        val spatialReference = MosaicGeometryESRI.getSRID(internalGeom.srid)
        MosaicPolygonESRI(new OGCPolygon(polygon, spatialReference))
    }

    override def fromSeq[T <: MosaicGeometry](geomSeq: Seq[T], geomType: GeometryTypeEnum.Value = POLYGON): MosaicPolygonESRI = {
        if (geomSeq.isEmpty) {
            // For empty sequence return an empty geometry with default Spatial Reference
            return MosaicPolygonESRI(new OGCPolygon(new Polygon(), MosaicGeometryESRI.defaultSpatialReference))
        }
        val spatialReference = SpatialReference.create(geomSeq.head.getSpatialReference)
        val newGeom = GeometryTypeEnum.fromString(geomSeq.head.getGeometryType) match {
            case POINT                         =>
                val extractedPoints = geomSeq.map(_.asInstanceOf[MosaicPointESRI])
                val exteriorRing = extractedPoints.map(_.coord).map(InternalCoord(_)).toArray
                val polygon = MosaicMultiPolygonESRI.createPolygon(Array(exteriorRing), Array(Array(Array())))
                new OGCPolygon(polygon, spatialReference)
            case LINESTRING                    =>
                val extractedLines = geomSeq.map(_.asInstanceOf[MosaicLineStringESRI])
                val exteriorRing = extractedLines.head.asSeq.map(_.coord).map(InternalCoord(_)).toArray
                val holes =
                    extractedLines.tail.map({ h: MosaicLineStringESRI => h.asSeq.map(_.coord).map(InternalCoord(_)).toArray }).toArray
                val polygon = MosaicMultiPolygonESRI.createPolygon(Array(exteriorRing), Array(holes))
                new OGCPolygon(polygon, spatialReference)
            case other: GeometryTypeEnum.Value => throw new UnsupportedOperationException(
                  s"MosaicGeometry.fromSeq() cannot create ${geomType.toString} from ${other.toString} geometries."
                )
        }
        MosaicPolygonESRI(newGeom)
    }

    override def fromWKB(wkb: Array[Byte]): MosaicGeometryESRI = MosaicGeometryESRI.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometryESRI = MosaicGeometryESRI.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometryESRI = MosaicGeometryESRI.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometryESRI = MosaicGeometryESRI.fromHEX(hex)

}
