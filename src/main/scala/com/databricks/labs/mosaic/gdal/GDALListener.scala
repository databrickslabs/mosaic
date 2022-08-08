package com.databricks.labs.mosaic.gdal

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorAdded}

class GDALListener extends SparkListener with Logging {

    override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
        super.onExecutorAdded(executorAdded)
        MosaicGDAL.installGDAL(None)
    }

}
