package com.databricks.labs.mosaic.core.crs

import java.io.InputStream

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI

/**
  * CRSBoundsProvider provides APIs to get bounds extreme points based on CRS
  * dataset name (ie. EPSG) and CRS id (ie. 4326). The lookup is not exhaustive
  * and it is generated based on a resource file. Resource file is sourced based
  * on spatialreference.org. Not all CRSs available at spatialreference.org have
  * bounds specified. Those are skipped in the resource file.
  * @see
  *   https://spatialreference.org/
  * @param lookup
  *   A map of (crs_dataset, id) -> (bounds, reprojected_bounds) pairs.
  */
case class CRSBoundsProvider(private val lookup: Map[(String, Int), (CRSBounds, CRSBounds)]) {

    /**
      * Returns bounds for provided CRS dataset and ID pair. Bounds are provided
      * as (longitude, latitude) points. Only lower left and upper right points
      * are supplied since they correspond to xmin, ymin and xmax and ymax
      * extremes.
      * @param dataset
      *   CRS dataset, e.g. EPSG.
      * @param id
      *   CRS id within the CRS dataset, e.g. 4326.
      * @return
      *   an instance of [[CRSBounds]] corresponding to supplied (crs_dataset,
      *   id) pair.
      */
    def bounds(dataset: String, id: Int): CRSBounds = {
        require(lookup.contains((dataset, id)), s"Requested CRS does not have boundaries defined: ${(dataset, id)}")
        lookup((dataset, id))._1
    }

    /**
      * Returns reprojected bounds for provided CRS dataset and ID pair. Bounds
      * are provided as (longitude, latitude) points equivalents. Only lower
      * left and upper right points are supplied since they correspond to xmin,
      * ymin and xmax and ymax extremes.
      * @param dataset
      *   CRS dataset, e.g. EPSG.
      * @param id
      *   CRS id within the CRS dataset, e.g. 27700.
      * @return
      *   an instance of [[CRSBounds]] corresponding to supplied (crs_dataset,
      *   id) pair.
      */
    def reprojectedBounds(dataset: String, id: Int): CRSBounds = {
        require(lookup.contains((dataset, id)), s"Requested CRS does not have boundaries defined: ${(dataset, id)}")
        lookup((dataset, id))._2
    }

}

object CRSBoundsProvider {

    /**
      * Creates an instance of [[CRSBoundsProvider]] based on a resource file
      * containing the bounds' lower left and upper right extreme points. The
      * lookup contains longitude and latitude bounds and reprojected equivalent
      * values. The bounds values have been sourced from spatialreference.org.
      * @see
      *   https://spatialreference.org/
      */
    def apply(geometryAPI: GeometryAPI): CRSBoundsProvider = {
        val stream: InputStream = getClass.getResourceAsStream("/CRSBounds.csv")
        val lines: List[String] = scala.io.Source.fromInputStream(stream).getLines.toList.drop(1)
        val lookupItems = lines
            .drop(1)
            .map(line => {
                val lineItems = line.split(",")
                val nameItems = lineItems(0).split(":")
                val (crsDataset, id) = (nameItems(0), nameItems(1).toInt)
                val (x1, y1, x2, y2) = (lineItems(1).toDouble, lineItems(2).toDouble, lineItems(3).toDouble, lineItems(4).toDouble)
                val (x3, y3, x4, y4) = (lineItems(5).toDouble, lineItems(6).toDouble, lineItems(7).toDouble, lineItems(8).toDouble)
                (crsDataset, id) -> (CRSBounds(geometryAPI, x1, y1, x2, y2), CRSBounds(geometryAPI, x3, y3, x4, y4))
            })
        val lookup = lookupItems.toMap
        CRSBoundsProvider(lookup)
    }

}
