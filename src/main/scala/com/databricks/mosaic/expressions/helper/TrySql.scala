package com.databricks.mosaic.expressions.helper

import java.io.{PrintWriter, StringWriter}

import scala.util._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

@ExpressionDescription(
  usage = "_FUNC_(expr1) - Wraps evaluation of an expression into a Try/Catch block and in case of errors returns error" +
      "message for each row in the dataset.",
  examples = """
    Examples:
      > SELECT _FUNC_(a);
       |row1|result|OK|
       |row2|result|Error message|
       |....
       |rowN|result|OK|
  """,
  since = "1.0"
)
case class TrySql(inExpr: Expression) extends UnaryExpression with CodegenFallback {

    /** Expression output DataType. */
    override def dataType: DataType = {
        val children = inExpr.children
        val childrenSchema = children
            .map(c => StructField(c.sql, c.dataType))
        StructType(
          Seq(
            StructField("inputs", StructType(childrenSchema)),
            StructField("result", inExpr.dataType),
            StructField("status", StringType)
          )
        )

    }

    override def toString: String = s"try_sql($inExpr)"

    /**
      * Overridden to ensure [[Expression.sql]] is properly formatted.
      */
    override def prettyName: String = "try_sql"

    override def eval(input: InternalRow): Any = {
        Try(
          (
            child.eval(input),
            child.children.map(ci => ci.eval(input))
          )
        ) match {
            case Success((result, inputs)) => InternalRow.fromSeq(Seq(InternalRow.fromSeq(inputs), result, UTF8String.fromString("OK")))
            case Failure(exception)        =>
                val inputs = child.children.map(ci => Try(ci.eval(input)).toOption.orNull)
                val sw = new StringWriter()
                exception.printStackTrace(new PrintWriter(sw))
                val exceptionAsString = sw.toString
                InternalRow.fromSeq(
                  Seq(
                    InternalRow.fromSeq(inputs),
                    null,
                    UTF8String.fromString(s"${exception.getMessage}; $exceptionAsString")
                  )
                )
        }
    }

    override def child: Expression = inExpr

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val res = TrySql(
          newArgs(0).asInstanceOf[Expression]
        )
        res.copyTagsFrom(this)
        res
    }

}
