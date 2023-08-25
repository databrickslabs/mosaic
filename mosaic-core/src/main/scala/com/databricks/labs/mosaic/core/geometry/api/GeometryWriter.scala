package com.databricks.labs.mosaic.core.geometry.api

/**
 * A trait that defines the methods for writing geometry data.
 * If a new format requires support, toFormat method should be added to this trait.
 */
trait GeometryWriter {

  def toWKB: Array[Byte]

  def toWKT: String

  def toJSON: String

  def toHEX: String

}
