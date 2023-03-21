package com.databricks.labs.mosaic.sql.extensions

import com.databricks.labs.mosaic.core.geometry.api.ESRI
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.core.raster.api.RasterAPI.GDAL
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions

/**
  * Supports automatic registration of SQL expressions at the cluster start up
  * time. This class registers and injects a rule that based on the default
  * index system and configured geometry api adds corresponding SQL expressions
  * to spark session. These rules are activated just after the spark session has
  * been created.
  */
class MosaicSQLDefault extends (SparkSessionExtensions => Unit) with Logging {

    /**
      * Constructor for the MosaicSQL extension. All the registration logic
      * happens before a NOP rule has been injected.
      * @param ext
      *   Hook to spark session that is needed for expression registration.
      */
    override def apply(ext: SparkSessionExtensions): Unit = {
        ext.injectCheckRule(spark => {
            val mosaicContext = MosaicContext.build(H3IndexSystem, ESRI, GDAL)
            logInfo(s"Registering Mosaic SQL Extensions (H3, ESRI, GDAL).")
            mosaicContext.register(spark)
            // NOP rule. This rule is specified only to respect syntax.
            _ => ()
        })
    }

}
