package com.databricks.labs.mosaic.sql.extensions

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions

class MosaicSQL extends (SparkSessionExtensions => Unit) with Logging {

    override def apply(ext: SparkSessionExtensions): Unit = {
        ext.injectCheckRule(spark => {
            val indexSystem = spark.conf.get("spark.databricks.labs.mosaic.index.system")
            val geometryAPI = spark.conf.get("spark.databricks.labs.mosaic.geometry.api")
            val mosaicContext = (indexSystem, geometryAPI) match {
                case (is, gapi) if is == H3IndexSystem.name && gapi == JTS.name   => MosaicContext.build(H3IndexSystem, JTS)
                case (is, gapi) if is == H3IndexSystem.name && gapi == ESRI.name  => MosaicContext.build(H3IndexSystem, ESRI)
                case (is, gapi) if is == BNGIndexSystem.name && gapi == JTS.name  => MosaicContext.build(BNGIndexSystem, JTS)
                case (is, gapi) if is == BNGIndexSystem.name && gapi == ESRI.name => MosaicContext.build(BNGIndexSystem, ESRI)
                case (is, gapi) => throw new IllegalArgumentException(s"Index system and geometry API: ($is, $gapi) not supported.")
            }
            logInfo(s"Registering Mosaic SQL Extensions ($indexSystem, $geometryAPI).")
            mosaicContext.register(spark)
            _ => ()
        })
    }

}
