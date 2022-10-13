package com.databricks.labs.mosaic.gdal

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerExecutorAdded}

class GDALListener extends SparkListener with Logging {

    override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
        super.onExecutorAdded(executorAdded)
        MosaicGDAL.installGDAL(None)
    }

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
        super.onApplicationEnd(applicationEnd)
        MosaicGDAL.disableGDAL()
    }

}
