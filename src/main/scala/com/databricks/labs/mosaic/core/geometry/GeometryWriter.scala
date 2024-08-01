package com.databricks.labs.mosaic.core.geometry

import com.databricks.labs.mosaic.core.types.model.InternalGeometry

trait GeometryWriter {

    def toInternal: InternalGeometry

    def toWKB: Array[Byte]

    def toWKB(coordDims: Int): Array[Byte]

    def toWKT: String

    def toWKT(coordDims: Int): String

    def toJSON: String

    def toHEX: String

}
