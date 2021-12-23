package com.databricks.mosaic.index

import com.databricks.mosaic.core.index.IndexSystemID
import com.databricks.mosaic.expressions.format.Conversions.geom2wkb
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.types._

@ExpressionDescription(
  usage = "_FUNC_(indexID, indexSystem) - Returns the geometry representing the index.",
  examples =
    """
    Examples:
      > SELECT _FUNC_(a, 'H3');
      0001100100100.....001010 // WKB
  """,
  since = "1.0")
case class IndexGeometry(indexID: Expression, indexSystemName: String)
  extends UnaryExpression with NullIntolerant with CodegenFallback {

  /** Expression output DataType. */
  override def dataType: DataType = BinaryType // Return WKB, if other type is required call ConvertTO

  override def toString: String = s"index_geometry($indexID)"

  /** Overridden to ensure [[Expression.sql]] is properly formatted. */
  override def prettyName: String = "index_geometry"

  /**
   * Computes the H3 index corresponding to the provided lat and long coordinates.
   * @param input1 Any instance containing the ID of the index.
   * @return H3 index id in Long.
   */
  override def nullSafeEval(input1: Any): Any = {
    val indexSystem = IndexSystemID.getIndexSystem(IndexSystemID(indexSystemName))
    val indexGeometry = indexSystem.indexToGeometry(input1.asInstanceOf[Long])
    indexGeometry.toWKB
  }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val arg1 = newArgs.head.asInstanceOf[Expression]
    val res = IndexGeometry(arg1, indexSystemName)
    res.copyTagsFrom(this)
    res
  }

  override def child: Expression = indexID
}
