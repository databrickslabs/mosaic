package com.databricks.labs.mosaic.sql.extensions

import com.databricks.labs.mosaic.gdal.MosaicGDAL
import com.databricks.labs.mosaic.MOSAIC_GDAL_NATIVE
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions

/**
  * Supports automatic registration of SQL expressions at the cluster start up
  * time. This class registers and injects a rule that based on the configured
  * index system and configured geometry api adds corresponding SQL expressions
  * to spark session. These rules are activated just after the spark session has
  * been created. If unsupported pair of index system and geometry api has been
  * provided this operation will crash the code in order to make sure correct
  * functionality has been enabled.
  */
class MosaicGDAL extends (SparkSessionExtensions => Unit) with Logging {

    /**
      * Constructor for the MosaicSQL extension. All the registration logic
      * happens before a nop rule has been injected.
      * @param ext
      *   Hook to spark session that is needed for expression registration.
      */
    override def apply(ext: SparkSessionExtensions): Unit = {
        ext.injectCheckRule(spark => {
            val enableGDAL = spark.conf.get(MOSAIC_GDAL_NATIVE, "false").toBoolean
            if (enableGDAL) {
                MosaicGDAL.enableGDAL(spark)
                logInfo(s"GDAL was installed successfully.")
            }
            // NOP rule. This rule is specified only to respect syntax.
            _ => ()
        })
    }

}
