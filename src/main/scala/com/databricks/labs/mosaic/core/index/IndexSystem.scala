package com.databricks.labs.mosaic.core.index

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.types.model.{Coordinates, GeometryTypeEnum, MosaicChip}
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.osr.SpatialReference

/**
  * Defines the API that all index systems need to respect for Mosaic to support
  * them.
  */
abstract class IndexSystem(var cellIdType: DataType) extends Serializable {

    // Passthrough if not redefined
    def isValid(cellID: Long): Boolean = true

    def isCylindrical: Boolean = false

    def crsID: Int

    /**
      * Returns the spatial reference of the index system. This is only
      * available when GDAL is available. For proj4j please use crsID method.
      *
      * @return
      *   SpatialReference
      */
    def osrSpatialRef: SpatialReference = {
        val sr = new SpatialReference()
        sr.ImportFromEPSG(crsID)
        sr.SetAxisMappingStrategy(org.gdal.osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        sr
    }

    def distance(cellId: Long, cellId2: Long): Long

    def getCellIdDataType: DataType = cellIdType

    def setCellIdDataType(dataType: DataType): Unit = {
        cellIdType = dataType
    }

    def getResolutionStr(resolution: Int): String

    def formatCellId(cellId: Any, dt: DataType): Any =
        (dt, cellId) match {
            case (LongType, _: Long)           => cellId
            case (LongType, cid: String)       => parse(cid)
            case (LongType, cid: UTF8String)   => parse(cid.toString)
            case (StringType, cid: Long)       => format(cid)
            case (StringType, cid: UTF8String) => cid.toString
            case (StringType, _: String)       => cellId
            case _                             => throw new Error("Cell ID data type not supported.")
        }

    def formatCellId(cellId: Any): Any = formatCellId(cellId, getCellIdDataType)

    def serializeCellId(cellId: Any): Any =
        (getCellIdDataType, cellId) match {
            case (LongType, _: Long)         => cellId
            case (LongType, cid: String)     => parse(cid)
            case (LongType, cid: UTF8String) => parse(cid.toString)
            case (StringType, cid: Long)     => UTF8String.fromString(format(cid))
            case (StringType, _: UTF8String) => cellId
            case (StringType, cid: String)   => UTF8String.fromString(cid)
            case _                           => throw new Error("Cell ID data type not supported.")
        }

    def format(id: Long): String

    def parse(id: String): Long

    /**
      * Get the k ring of indices around the provided index id.
      *
      * @param index
      *   Index ID to be used as a center of k ring.
      * @param n
      *   Number of k rings to be generated around the input index.
      * @return
      *   A collection of index IDs forming a k ring.
      */
    def kRing(index: Long, n: Int): Seq[Long]

    def kRing(index: String, n: Int): Seq[String] = kRing(parse(index), n).map(format)

    /**
      * Get the k loop (hollow ring) of indices around the provided index id.
      *
      * @param index
      *   Index ID to be used as a center of k loop.
      * @param n
      *   Distance of k loop to be generated around the input index.
      * @return
      *   A collection of index IDs forming a k loop.
      */
    def kLoop(index: Long, n: Int): Seq[Long]

    def kLoop(index: String, n: Int): Seq[String] = kLoop(parse(index), n).map(format)

    /**
      * Returns the set of supported resolutions for the given index system.
      * This doesnt have to be a continuous set of values. Only values provided
      * in this set are considered valid.
      *
      * @return
      *   A set of supported resolutions.
      */
    def resolutions: Set[Int]

    /**
      * Returns the name of the IndexSystem.
      *
      * @return
      *   IndexSystem name.
      */
    def name: String

    /**
      * Returns the resolution value based on the nullSafeEval method inputs of
      * type Any. Each Index System should ensure that only valid values of
      * resolution are accepted.
      *
      * @param res
      *   Any type input to be parsed into the Int representation of resolution.
      * @return
      *   Int value representing the resolution.
      */
    @throws[IllegalStateException]
    def getResolution(res: Any): Int

    /**
      * Computes the radius of minimum enclosing circle of the polygon
      * corresponding to the centroid index of the provided geometry.
      *
      * @param geometry
      *   An instance of [[MosaicGeometry]] for which we are computing the
      *   optimal buffer radius.
      * @param resolution
      *   A resolution to be used to get the centroid index geometry.
      * @return
      *   An optimal radius to buffer the geometry in order to avoid blind spots
      *   when performing polyfill.
      */
    def getBufferRadius(geometry: MosaicGeometry, resolution: Int, geometryAPI: GeometryAPI): Double

    def alignToGrid(geometry: MosaicGeometry): MosaicGeometry = geometry

    /**
      * Returns a set of indices that represent the input geometry. Depending on
      * the index system this set may include only indices whose centroids fall
      * inside the input geometry or any index that intersects the input
      * geometry. When extending make sure which is the guaranteed behavior of
      * the index system.
      *
      * @param geometry
      *   Input geometry to be represented.
      * @param resolution
      *   A resolution of the indices.
      * @return
      *   A set of indices representing the input geometry.
      */
    def polyfill(geometry: MosaicGeometry, resolution: Int, geometryAPI: GeometryAPI): Seq[Long]

    /**
      * @see
      *   [[IndexSystem.getBorderChips()]]
      * @param geometry
      *   Input geometry whose border is being represented.
      * @param borderIndices
      *   Indices corresponding to the border area of the input geometry.
      * @return
      *   A border area representation via [[MosaicChip]] set.
      */
    def getBorderChips(
        geometry: MosaicGeometry,
        borderIndices: Seq[Long],
        keepCoreGeom: Boolean,
        geometryAPI: GeometryAPI
    ): Seq[MosaicChip] = {
        val intersections = for (index <- borderIndices) yield {
            val indexGeom = indexToGeometry(index, geometryAPI)
            val intersect = geometry.intersection(indexGeom)
            val coerced = coerceChipGeometry(intersect, indexGeom, geometry)
            val isCore = coerced.equals(indexGeom)

            val chipGeom = if (!isCore || keepCoreGeom) coerced else null

            MosaicChip(isCore = isCore, Left(index), chipGeom)
        }
        intersections.filterNot(_.isEmpty)
    }

    /**
      * Return a set of [[MosaicChip]] instances computed based on the core
      * indices. Each index is converted to an instance of [[MosaicChip]]. These
      * chips do not contain chip geometry since they are full contained by the
      * geometry whose core they represent.
      *
      * @param coreIndices
      *   Indices corresponding to the core area of the input geometry.
      * @return
      *   A core area representation via [[MosaicChip]] set.
      */
    def getCoreChips(coreIndices: Seq[Long], keepCoreGeom: Boolean, geometryAPI: GeometryAPI): Seq[MosaicChip] = {
        coreIndices.map(index => {
            val indexGeom = if (keepCoreGeom) indexToGeometry(index, geometryAPI) else null
            MosaicChip(isCore = true, Left(index), indexGeom)
        })
    }

    /**
      * Get the geometry corresponding to the index with the input id.
      *
      * @param index
      *   Id of the index whose geometry should be returned.
      * @return
      *   An instance of [[MosaicGeometry]] corresponding to index.
      */
    def indexToGeometry(index: Long, geometryAPI: GeometryAPI): MosaicGeometry

    /**
      * Get the index ID corresponding to the provided coordinates.
      *
      * @param lon
      *   X coordinate of the point.
      * @param lat
      *   Y coordinate of the point.
      * @param resolution
      *   Resolution of the index.
      * @return
      *   Index ID in this index system.
      */
    def pointToIndex(lon: Double, lat: Double, resolution: Int): Long

    def indexToCenter(index: Long): Coordinates

    def indexToCenter(index: String): Coordinates = indexToCenter(parse(index))

    def indexToBoundary(index: Long): Seq[Coordinates]

    def indexToBoundary(index: String): Seq[Coordinates] = indexToBoundary(parse(index))

    // ASSUMPTION: index cells are convex. If index cells are not convex, you must override this method
    def area(index: Long): Double = {
        // Haversine distance between two coordinates in radians
        def haversine(coords1: Coordinates, coords2: Coordinates): Double = {
            val c = math.Pi / 180

            val th1 = c * coords1.lat
            val th2 = c * coords2.lat
            val dph = c * (coords1.lng - coords2.lng)

            val dz = math.sin(th1) - math.sin(th2)
            val dx = math.cos(dph) * math.cos(th1) - math.cos(th2)
            val dy = math.sin(dph) * math.cos(th1)

            math.asin(math.sqrt(dx * dx + dy * dy + dz * dz) / 2) * 2
        }

        def triangle_area(boundary_coords: Seq[Coordinates], center_coord: Coordinates): Double = {
            val a = haversine(center_coord, boundary_coords(0));
            val b = haversine(boundary_coords(0), boundary_coords(1));
            val c = haversine(boundary_coords(1), center_coord);

            val s = (a + b + c) / 2;
            val t = math.sqrt(
              math.tan(s / 2)
                  * math.tan((s - a) / 2)
                  * math.tan((s - b) / 2)
                  * math.tan((s - c) / 2)
            );

            val e = 4 * math.atan(t);

            val r = 6371.0088;
            val area = e * r * r
            area
        }

        val center = indexToCenter(index);
        val boundary = indexToBoundary(index);
        val boundary_ring = boundary ++ Seq(boundary.head);
        val res = boundary_ring.sliding(2).map(b => triangle_area(b, center)).sum
        res
    }

    def area(index: String): Double = area(parse(index))

    def coerceChipGeometry(geom: MosaicGeometry, indexGeom: MosaicGeometry, originGeom: MosaicGeometry): MosaicGeometry = {
        val geomType = GeometryTypeEnum.fromString(geom.getGeometryType)
        val originGeomType = GeometryTypeEnum.fromString(originGeom.getGeometryType)
        if (geomType == GEOMETRYCOLLECTION || geomType != originGeomType) {
            // This case can occur if partial geometry is a geometry collection
            // or if the intersection includes a part of the boundary of the cell
            geom.difference(indexGeom.getBoundary)
        } else {
            geom
        }
    }

    def coerceChipGeometry(geometries: Seq[MosaicGeometry]): Seq[MosaicGeometry] = {
        val types = geometries.map(_.getGeometryType).map(GeometryTypeEnum.fromString)
        if (types.contains(MULTIPOLYGON) || types.contains(POLYGON)) {
            geometries.filter(g => Seq(POLYGON, MULTIPOLYGON).contains(GeometryTypeEnum.fromString(g.getGeometryType)))
        } else if (types.contains(MULTILINESTRING) || types.contains(LINESTRING)) {
            geometries.filter(g => Seq(MULTILINESTRING, LINESTRING).contains(GeometryTypeEnum.fromString(g.getGeometryType)))
        } else if (types.contains(MULTIPOINT) || types.contains(POINT)) {
            geometries.filter(g => Seq(MULTIPOINT, POINT).contains(GeometryTypeEnum.fromString(g.getGeometryType)))
        } else {
            Nil
        }
    }

}
