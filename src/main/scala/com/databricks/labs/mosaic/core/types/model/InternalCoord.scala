package com.databricks.labs.mosaic.core.types.model

import org.locationtech.jts.geom.Coordinate

import org.apache.spark.sql.catalyst.util.ArrayData

/**
  * A case class modeling 2D or 3D point instances. Coordinates are stored as an
  * array of doubles to accommodate for variable number of dimensions.
  *
  * @param coords
  *   A sequence of coordinates.
  */
case class InternalCoord(coords: Seq[Double]) {

    /**
      * Serialize for spark internals.
      *
      * @return
      *   An instance of [[ArrayData]].
      */
    def serialize: ArrayData = ArrayData.toArrayData(coords)

    /**
      * Convert to JTS [[Coordinate]] instance.
      *
      * @return
      *   An instance of [[Coordinate]].
      */
    // noinspection ZeroIndexToHead
    def toCoordinate: Coordinate = {
        if (coords.length == 2) {
            new Coordinate(coords(0), coords(1))
        } else {
            new Coordinate(coords(0), coords(1), coords(2))
        }
    }

}

/** Companion object. */
object InternalCoord {

    /**
      * Smart constructor based on JTS [[Coordinate]] instance.
      *
      * @param coordinate
      *   An instance of [[Coordinate]].
      * @return
      *   An instance of [[InternalCoord]].
      */
    def apply(coordinate: Coordinate): InternalCoord = {
        val z = coordinate.getZ
        if (z.isNaN) {
            new InternalCoord(Seq(coordinate.getX, coordinate.getY))
        } else {
            new InternalCoord(Seq(coordinate.getX, coordinate.getY, z))
        }
    }

    /**
      * Smart constructor based on Spark internal instance.
      *
      * @param input
      *   An instance of [[ArrayData]].
      * @return
      *   An instance of [[InternalCoord]].
      */
    def apply(input: ArrayData): InternalCoord = {
        if (input.numElements() == 2) {
            new InternalCoord(Seq(input.getDouble(0), input.getDouble(1)))
        } else {
            new InternalCoord(Seq(input.getDouble(0), input.getDouble(1), input.getDouble(2)))
        }
    }

}
