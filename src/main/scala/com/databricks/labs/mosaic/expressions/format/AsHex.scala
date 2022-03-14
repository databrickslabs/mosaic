package com.databricks.labs.mosaic.expressions.format

import com.databricks.labs.mosaic.codegen.format.InternalTypeWrapper
import com.databricks.labs.mosaic.core.types.HexType

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{DataType, StringType}

case class AsHex(inGeometry: Expression) extends UnaryExpression with NullIntolerant {

    /**
      * AsHex expression wraps string payload into a StructType. This wrapping
      * ensures we can differentiate between StringType (WKT) and HexType. Only
      * StringType is accepted as input data type.
      */
    override def checkInputDataTypes(): TypeCheckResult =
        inGeometry.dataType match {
            case StringType => TypeCheckResult.TypeCheckSuccess
            case _          => TypeCheckResult.TypeCheckFailure(
                  s"Cannot cast to HEX from ${inGeometry.dataType.sql}! Only String Columns are supported."
                )
        }

    /** Expression output DataType. */
    override def dataType: DataType = HexType

    override def toString: String = s"as_hex($inGeometry)"

    /** Overridden to ensure [[Expression.sql]] is properly formatted. */
    override def prettyName: String = "as_hex"

    /**
      * AsHex expression wraps string payload into a StructType. This wrapping
      * ensures we can differentiate between StringType (WKT) and HexType.
      */
    override def nullSafeEval(input: Any): Any =
        inGeometry.dataType match {
            case StringType => InternalRow.fromSeq(Seq(input))
            case _          => throw new Error(
                  s"Cannot cast to HEX from ${inGeometry.dataType.sql}! Only String Columns are supported."
                )
        }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val res = AsHex(
          newArgs(0).asInstanceOf[Expression]
        )
        res.copyTagsFrom(this)
        res
    }

    override def child: Expression = inGeometry

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
        InternalTypeWrapper.doGenCode(ctx, ev, this.nullSafeCodeGen)

    override protected def withNewChildInternal(newChild: Expression): Expression = copy(inGeometry = newChild)

}

object AsHex {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[AsHex].getCanonicalName,
          db.orNull,
          "as_hex",
          "_FUNC_(col1) - Wraps the column in a fixed struct for type inference.",
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a);
            |       {"00001005FA...00A"} // random hex content provided for illustration only
            |  """.stripMargin,
          "",
          "struct_funcs",
          "1.0",
          "",
          "built-in"
        )
}
