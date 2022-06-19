package com.databricks.labs.mosaic.sql.extensions

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.ESRI
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions

class MosaicSQLDefault extends (SparkSessionExtensions => Unit) with Logging {

    override def apply(ext: SparkSessionExtensions): Unit = {
        ext.injectCheckRule(spark => {
            val mosaicContext = MosaicContext.build(H3IndexSystem, ESRI)
            logInfo(s"Registering Mosaic SQL Extensions (H3, ESRI).")
            mosaicContext.register(spark)
            _ => ()
        })
    }

}
