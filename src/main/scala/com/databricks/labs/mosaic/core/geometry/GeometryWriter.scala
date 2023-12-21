package com.databricks.labs.mosaic.core.geometry

trait GeometryWriter {

    def toWKB: Array[Byte]

    def toWKT: String

    def toJSON: String

    def toHEX: String

}
