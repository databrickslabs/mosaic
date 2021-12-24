package com.databricks.mosaic.expressions.index

import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

import com.databricks.mosaic.core.geometry.GeometryAPI
import com.databricks.mosaic.core.index.IndexSystemID

@ExpressionDescription(
  usage = "_FUNC_(indexID, indexSystem) - Returns the geometry representing the index.",
  examples =
    """
    Examples:
      > SELECT _FUNC_(a, 'H3');
      0001100100100.....001010 // WKB
  """,
  since = "1.0")
case class IndexGeometry(indexID: Expression, indexSystemName: String, geometryAPIName: String)
  extends UnaryExpression with NullIntolerant with CodegenFallback {

  /** Expression output DataType. */
  // Return WKB, if other type is required call ConvertTO
  override def dataType: DataType = BinaryType

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
    val geometryAPI = GeometryAPI(geometryAPIName)
    val indexGeometry = indexSystem.indexToGeometry(input1.asInstanceOf[Long], geometryAPI)
    indexGeometry.toWKB
  }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val arg1 = newArgs.head.asInstanceOf[Expression]
    val res = IndexGeometry(arg1, indexSystemName, geometryAPIName)
    res.copyTagsFrom(this)
    res
  }

  override def child: Expression = indexID
}
