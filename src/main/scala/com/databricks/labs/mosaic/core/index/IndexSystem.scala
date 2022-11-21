package com.databricks.labs.mosaic.core.index

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.types.model.MosaicChip
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
  * Defines the API that all index systems need to respect for Mosaic to support
  * them.
  */
abstract class IndexSystem(var cellIdType: DataType) extends Serializable {

    def getCellIdDataType: DataType = cellIdType

    def setCellIdDataType(dataType: DataType): Unit = {
        cellIdType = dataType
    }

    def getResolutionStr(resolution: Int): String

    def formatCellId(cellId: Any, dt: DataType): Any = (dt, cellId) match {
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
    def name: String = getIndexSystemID.name

    /**
      * Returns the index system ID instance that uniquely identifies an index
      * system. This instance is used to select appropriate Mosaic expressions.
      *
      * @return
      *   An instance of [[IndexSystemID]]
      */
    def getIndexSystemID: IndexSystemID

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
    def polyfill(geometry: MosaicGeometry, resolution: Int, geometryAPI: Option[GeometryAPI] = None): Seq[Long]

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
            val isCore = intersect.equals(indexGeom)

            val chipGeom = if (!isCore || keepCoreGeom) intersect else null

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
      * Get the geometry corresponding to the index with the input id.
      *
      * @param index
      *   Id of the index whose geometry should be returned.
      * @return
      *   An instance of [[MosaicGeometry]] corresponding to index.
      */
    def indexToGeometry(index: String, geometryAPI: GeometryAPI): MosaicGeometry

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

}
