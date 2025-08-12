package com.dblabs.spatial.expressions

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SerializableConfiguration

case class ExpressionConfig(
    configs: Map[String, String],
    hConf: SerializableConfiguration
) extends Serializable {

    def getGDALConfig: Map[String, String] = {
        configs.filter(p => {
            p._1.startsWith("spark.dblabs.spatial.gdal.") ||
            p._1.startsWith("spark.gdal.")
        })
    }

    def getSharedObjects: Map[String, String] = {
        configs.filter(p => {
            p._1.startsWith("spark.dblabs.spatial.sharedobjects.") ||
            p._1.startsWith("spark.sharedobjects.")
        })
    }

    def getRasterCheckpointDir: String = {
        configs.getOrElse("spark.dblabs.spatial.raster.checkpoint.dir", "/tmp/raster-checkpoint")
    }

    def useCheckpoint: Boolean = {
        configs.getOrElse("spark.dblabs.spatial.raster.use.checkpoint", "false").toBoolean
    }

}

object ExpressionConfig {

    def apply(spark: SparkSession): ExpressionConfig = {
        new ExpressionConfig(
          spark.conf.getAll,
          new SerializableConfiguration(spark.sessionState.newHadoopConf())
        )
    }

}
