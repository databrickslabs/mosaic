package org.apache.spark.sql.adapters

import org.apache.spark.deploy.SparkHadoopUtil

object SparkHadoopUtils {

    def sdu: SparkHadoopUtil = SparkHadoopUtil.get

}
