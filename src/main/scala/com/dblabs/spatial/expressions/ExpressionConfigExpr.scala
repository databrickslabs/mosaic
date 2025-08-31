package com.dblabs.spatial.expressions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, RuntimeReplaceable}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

case class ExpressionConfigExpr() extends RuntimeReplaceable {

    override def nullable: Boolean = false
    override def dataType: DataType = StringType
    override def children: Seq[Expression] = Nil
    override def prettyName: String = "expr_config_expr"

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = ExpressionConfigExpr()

    override lazy val replacement: Expression = {
        val spark = SparkSession.getActiveSession.get
        val exprConf = ExpressionConfig(spark)
        val b64 = exprConf.toB64
        new Literal(UTF8String.fromString(b64), StringType) {
            override def toString(): String = "literal(configs[REDACTED])"
        }
    }

}
