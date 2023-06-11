package com.databricks.labs.mosaic.core

import org.apache.spark.sql.types._

/**
 * Contains definition of all Mosaic specific data types. It provides methods
 * for type inference over geometry columns.
 */
package object types {

  val HexType: DataType = new HexType()
  val GeoJSONType: DataType = new GeoJSONType()

}
