package com.databricks.mosaic.expressions.index

import com.databricks.mosaic.core.geometry.api.GeometryAPI
import com.databricks.mosaic.core.index.{H3IndexSystem, IndexSystemID}
import com.databricks.mosaic.core.types.{HexType, InternalGeometryType}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, ExpectsInputTypes, Expression, ExpressionDescription, NullIntolerant}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

@ExpressionDescription(
  usage = "_FUNC_(geometry, resolution) - Returns the 1 set representation of geometry at resolution.",
  examples =
    """
    Examples:
      > SELECT _FUNC_(a, b);
       [622236721348804607, 622236721274716159, ...]
  """,
  since = "1.0")
case class Polyfill(geom: Expression, resolution: Expression, indexSystemName: String, geometryAPIName: String)
  extends BinaryExpression with ExpectsInputTypes with NullIntolerant with CodegenFallback {

  //noinspection DuplicatedCode
  override def inputTypes: Seq[DataType] = (left.dataType, right.dataType) match {
    case (BinaryType, IntegerType) => Seq(BinaryType, IntegerType)
    case (StringType, IntegerType) => Seq(StringType, IntegerType)
    case (HexType, IntegerType) => Seq(HexType, IntegerType)
    case (InternalGeometryType, IntegerType) => Seq(InternalGeometryType, IntegerType)
    case _ => throw new IllegalArgumentException(s"Not supported data type: (${left.dataType}, ${right.dataType}).")
  }

  /** Expression output DataType. */
  override def dataType: DataType = ArrayType(LongType)

  override def toString: String = s"h3_polyfill($geom, $resolution)"

  /** Overridden to ensure [[Expression.sql]] is properly formatted. */
  override def prettyName: String = "h3_polyfill"

  /**
   * Generates a set of indices corresponding to H3 polyfill call over
   * the input geometry.
   *
   * @param input1 Any instance containing the geometry.
   * @param input2 Any instance containing the resolution.
   * @return A set of H3 indices.
   */
  //noinspection DuplicatedCode
  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val resolution: Int = H3IndexSystem.getResolution(input2)

    val indexSystem = IndexSystemID.getIndexSystem(IndexSystemID(indexSystemName))
    val geometryAPI  = GeometryAPI(geometryAPIName)
    val geometry = geometryAPI.geometry(input1, left.dataType)
    val indices  = indexSystem.polyfill(geometry, resolution)

    val serialized = ArrayData.toArrayData(indices.toArray)
    serialized
  }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
    val res = Polyfill(asArray(0), asArray(1), indexSystemName, geometryAPIName)
    res.copyTagsFrom(this)
    res
  }

  override def left: Expression = geom

  override def right: Expression = resolution
}
