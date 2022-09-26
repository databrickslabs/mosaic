package com.databricks.labs

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem

package object mosaic {

    val JTS: GeometryAPI = mosaic.core.geometry.api.GeometryAPI.JTS
    val ESRI: GeometryAPI = mosaic.core.geometry.api.GeometryAPI.ESRI
    val H3: IndexSystem = mosaic.core.index.H3IndexSystem
    val BNG: IndexSystem = mosaic.core.index.BNGIndexSystem

    val DATABRICKS_SQL_FUNCTIONS_MODULE = "com.databricks.sql.functions"
    val SPARK_DATABRICKS_GEO_H3_ENABLED = "spark.databricks.geo.h3.enabled"

}
