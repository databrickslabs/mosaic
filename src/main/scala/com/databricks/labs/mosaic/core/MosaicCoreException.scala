package com.databricks.labs.mosaic.core

object MosaicCoreException {

    def InvalidGeometryOperation(message: String = "The operation should not be called on this geometry type."): Exception =
        new Exception(message)

}
