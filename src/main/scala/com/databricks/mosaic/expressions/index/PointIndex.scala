package com.databricks.mosaic.expressions.index

import com.databricks.mosaic.core.index.{H3IndexSystem, IndexSystemID}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, NullIntolerant, TernaryExpression}
import org.apache.spark.sql.types._

@ExpressionDescription(
  usage = "_FUNC_(lat, lng, resolution) - Returns the h3 index of a point(lat, lng) at resolution.",
  examples =
    """
    Examples:
      > SELECT _FUNC_(a, b, 10);
      622236721348804607
  """,
  since = "1.0")
case class PointIndex(lat: Expression, lng: Expression, resolution: Expression, indexSystemName: String)
  extends TernaryExpression with ExpectsInputTypes with NullIntolerant with CodegenFallback {

  override def inputTypes: Seq[DataType] = Seq(DoubleType, DoubleType, IntegerType)

  /** Expression output DataType. */
  override def dataType: DataType = LongType

  override def toString: String = s"h3_point_index($lat, $lng, $resolution)"

  /** Overridden to ensure [[Expression.sql]] is properly formatted. */
  override def prettyName: String = "h3_point_index"

  /**
   * Computes the H3 index corresponding to the provided lat and long coordinates.
   *
   * @param input1 Any instance containing latitude.
   * @param input2 Any instance containing longitude.
   * @param input3 Any instance containing resolution.
   * @return H3 index id in Long.
   */
  override def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
    val resolution: Int = H3IndexSystem.getResolution(input3)
    val x: Double = input1.asInstanceOf[Double]
    val y: Double = input2.asInstanceOf[Double]

    val indexSystem = IndexSystemID.getIndexSystem(IndexSystemID(indexSystemName))

    indexSystem.pointToIndex(x, y, resolution)
  }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val asArray = newArgs.take(3).map(_.asInstanceOf[Expression])
    val res = PointIndex(asArray(0), asArray(1), asArray(2), indexSystemName)
    res.copyTagsFrom(this)
    res
  }

  override def children: Seq[Expression] = Seq(lat, lng, resolution)
}
