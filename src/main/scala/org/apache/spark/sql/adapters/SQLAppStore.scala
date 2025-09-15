package org.apache.spark.sql.adapters

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ui.SQLAppStatusStore

object SQLAppStore {

    def get(sparkSession: SparkSession): SQLAppStatusStore = {
        new SQLAppStatusStore(sparkSession.sparkContext.statusStore.store)
    }

}
