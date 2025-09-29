package com.databricks.labs.gbx.expressions

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.adapters.SparkHadoopUtils
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, RuntimeReplaceable}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

case class ExpressionConfigExpr() extends RuntimeReplaceable {

    override def dataType: DataType = StringType
    override def children: Seq[Expression] = Nil
    override def prettyName: String = "expr_config_expr"

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = ExpressionConfigExpr()

    override lazy val replacement: Expression = {
        val activeSession = SparkSession.getActiveSession
        val defaultSession = SparkSession.getDefaultSession
        val fromSession = activeSession.orElse(defaultSession).map(s => ExpressionConfig(s).toB64)

        val b64 = fromSession.getOrElse {
            val conf = SparkEnv.get.conf
            val hConf = SparkHadoopUtils.sdu.newConfiguration(conf)
            new ExpressionConfig(conf.getAll.toMap, new SerializableConfiguration(hConf)).toB64
        }

        new Literal(UTF8String.fromString(b64), StringType) {
            override def toString(): String = "literal(configs[REDACTED])"
        }
    }

}
