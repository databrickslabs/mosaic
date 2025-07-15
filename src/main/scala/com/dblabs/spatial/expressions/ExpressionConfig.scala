package com.dblabs.spatial.expressions

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SerializableConfiguration

case class ExpressionConfig(
    configs: Map[String, String],
    hConf: SerializableConfiguration
) extends Serializable {}

object ExpressionConfig {

    def apply(spark: SparkSession): ExpressionConfig = {
        new ExpressionConfig(
          spark.conf.getAll,
          new SerializableConfiguration(spark.sessionState.newHadoopConf())
        )
    }

}
