package com.databricks.mosaic.core.geometry

import com.databricks.mosaic.core.types.model.InternalGeometry

trait GeometryWriter {

    def toInternal: InternalGeometry

    def toWKB: Array[Byte]

    def toWKT: String

    def toJSON: String

    def toHEX: String

    def toKryo: Array[Byte]

}
