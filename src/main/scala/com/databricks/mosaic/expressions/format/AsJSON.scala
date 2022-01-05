package com.databricks.mosaic.expressions.format

import com.databricks.mosaic.core.types.JSONType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.types.{DataType, StringType}

@ExpressionDescription(
  usage = "_FUNC_(expr1) - Returns the GeoJSON string representation wrapped into a struct for a type matching purposes.",
  examples =
    """
    Examples:
      > SELECT _FUNC_(a);
       {"{"type":"Polygon","coordinates":[[[30,10]...]]}"}
  """,
  since = "1.0")
case class AsJSON(inGeometry: Expression)
  extends UnaryExpression with NullIntolerant with CodegenFallback {

  /**
   * AsHex expression wraps string payload into a StructType.
   * This wrapping ensures we can differentiate between StringType (WKT) and JSONType.
   * Only StringType is accepted as input data type.
   */
  override def checkInputDataTypes(): TypeCheckResult =
    inGeometry.dataType match {
      case StringType => TypeCheckResult.TypeCheckSuccess
      case _ =>
        TypeCheckResult.TypeCheckFailure(
          s"Cannot cast to GeoJSON from ${inGeometry.dataType.sql}! Only String Columns are supported."
        )
    }

  /** Expression output DataType. */
  override def dataType: DataType = JSONType

  override def toString: String = s"as_json($inGeometry)"

  /** Overridden to ensure [[Expression.sql]] is properly formatted. */
  override def prettyName: String = "as_json"

  /**
   * AsHex expression wraps string payload into a StructType.
   * This wrapping ensures we can differentiate between StringType (WKT) and JSONType.
   */
  override def nullSafeEval(input: Any): Any =
    inGeometry.dataType match {
      case StringType =>
        val x = InternalRow.fromSeq(Seq(input))
        x
      case _ =>
        throw new Error(
          s"Cannot cast to GeoJSON from ${inGeometry.dataType.sql}! Only String Columns are supported."
        )
    }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val res = AsJSON(
      newArgs(0).asInstanceOf[Expression]
    )
    res.copyTagsFrom(this)
    res
  }

  override def child: Expression = inGeometry
}
