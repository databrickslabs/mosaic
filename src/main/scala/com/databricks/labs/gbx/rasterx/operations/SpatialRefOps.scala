package com.databricks.labs.gbx.rasterx.operations

import org.gdal.osr.SpatialReference

object SpatialRefOps {

    def getEPSGCode(spatialRef: SpatialReference): Int = {
        (spatialRef.GetAuthorityName(null), spatialRef.GetAuthorityCode(null)) match {
            case (null, _)                                      => 0
            case (name: String, code: String) if name == "EPSG" => code.toInt
            case _                                              => 0
        }
    }

    def fromEPSGCode(getSRID: Int): SpatialReference = {
        val sr = new SpatialReference()
        if (getSRID > 0) {
            sr.ImportFromEPSG(getSRID)
        } else {
            sr.SetWellKnownGeogCS("WGS84") // Default to WGS84 if no valid EPSG code
        }
        sr
    }

}
