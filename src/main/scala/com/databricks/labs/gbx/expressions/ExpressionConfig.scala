package com.databricks.labs.gbx.expressions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

case class ExpressionConfig(
    configs: Map[String, String],
    hConf: SerializableConfiguration
) extends Serializable {

    def toB64: String = {
        val bos = new java.io.ByteArrayOutputStream()
        val oos = new java.io.ObjectOutputStream(bos)
        oos.writeObject(this); oos.close()
        java.util.Base64.getEncoder.encodeToString(bos.toByteArray)
    }

    def getGDALConfig: Map[String, String] = {
        configs.filter(p => {
            p._1.startsWith("spark.databricks.labs.gbx.gdal.") ||
            p._1.startsWith("spark.gdal.")
        })
    }

    def getSharedObjects: Map[String, String] = {
        configs.filter(p => {
            p._1.startsWith("spark.databricks.labs.gbx.sharedobjects.") ||
            p._1.startsWith("spark.sharedobjects.")
        })
    }

    def getRasterCheckpointDir: String = {
        configs.getOrElse("spark.databricks.labs.gbx.raster.checkpoint.dir", "/tmp/raster-checkpoint")
    }

    def useCheckpoint: Boolean = {
        configs.getOrElse("spark.databricks.labs.gbx.raster.use.checkpoint", "false").toBoolean
    }

    def crashExpressions: Boolean = {
        configs.getOrElse("spark.databricks.labs.gbx.expressions.crash.on.error", "false").toBoolean
    }

}

object ExpressionConfig {

    def apply(spark: SparkSession): ExpressionConfig = {
        new ExpressionConfig(
          spark.conf.getAll,
          new SerializableConfiguration(spark.sessionState.newHadoopConf())
        )
    }

    def fromB64(b64: String): ExpressionConfig = {
        val bytes = java.util.Base64.getDecoder.decode(b64)
        val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes))
        ois.readObject().asInstanceOf[ExpressionConfig]
    }

    def fromExpr(expr: Expression): ExpressionConfig = {
        val b64 = expr.eval(null).asInstanceOf[UTF8String]
        fromB64(b64.toString)
    }

}
