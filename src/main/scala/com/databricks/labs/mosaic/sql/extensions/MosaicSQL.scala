package com.databricks.labs.mosaic.sql.extensions

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
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
class MosaicSQL extends (SparkSessionExtensions => Unit) with Logging {

    /**
      * Constructor for the MosaicSQL extension. All the registration logic
      * happens before a nop rule has been injected.
      * @param ext
      *   Hook to spark session that is needed for expression registration.
      */
    override def apply(ext: SparkSessionExtensions): Unit = {
        ext.injectCheckRule(spark => {
            val indexSystem = spark.conf.get("spark.databricks.labs.mosaic.index.system")
            val geometryAPI = spark.conf.get("spark.databricks.labs.mosaic.geometry.api")
            val mosaicContext = (indexSystem, geometryAPI) match {
                case ("H3", "JTS")   => MosaicContext.build(H3IndexSystem, JTS)
                case ("H3", "ESRI")  => MosaicContext.build(H3IndexSystem, ESRI)
                case ("BNG", "JTS")  => MosaicContext.build(BNGIndexSystem, JTS)
                case ("BNG", "ESRI") => MosaicContext.build(BNGIndexSystem, ESRI)
                case (is, gapi)      => throw new Error(s"Index system and geometry API: ($is, $gapi) not supported.")
            }
            logInfo(s"Registering Mosaic SQL Extensions ($indexSystem, $geometryAPI).")
            mosaicContext.register(spark)
            // NOP rule. This rule is specified only to respect syntax.
            _ => ()
        })
    }

}
