package com.databricks.labs

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.api.RasterAPI

package object mosaic {

    val JTS: GeometryAPI = mosaic.core.geometry.api.GeometryAPI.JTS
    val ESRI: GeometryAPI = mosaic.core.geometry.api.GeometryAPI.ESRI
    val GDAL: RasterAPI = mosaic.core.raster.api.RasterAPI.GDAL
    val H3: IndexSystem = mosaic.core.index.H3IndexSystem
    val BNG: IndexSystem = mosaic.core.index.BNGIndexSystem

}
