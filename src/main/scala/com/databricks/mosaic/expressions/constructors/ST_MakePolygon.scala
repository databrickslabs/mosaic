package com.databricks.mosaic.expressions.constructors

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType

import com.databricks.mosaic.core.types.InternalGeometryType
import com.databricks.mosaic.core.types.model.{GeometryTypeEnum, InternalGeometry}


@ExpressionDescription(
  usage =
    "_FUNC_(expr1) - Creates a new Polygon geometry from a closed LineString.",
  examples =
    """
    Examples:
      > SELECT _FUNC_(A);
  """,
  since = "1.0"
  )
case class ST_MakePolygon(boundaryRing: Expression)
  extends UnaryExpression
    with NullIntolerant
    with CodegenFallback {
  override def child: Expression = boundaryRing

  override def dataType: DataType = InternalGeometryType

  def isClosed(lineString: InternalGeometry): Boolean =
    lineString.boundaries.head.head == lineString.boundaries.head.last

  override def eval(input: InternalRow): Any = {
    val ring = InternalGeometry(boundaryRing.eval(input).asInstanceOf[InternalRow])
    if (isClosed(ring)) {
      val polygon = new InternalGeometry(GeometryTypeEnum.POLYGON.id, ring.boundaries, Array(Array()))
      polygon.serialize
    } else {
      throw new IllegalArgumentException("LineString supplied to ST_MakePolygon is not closed.")
    }
    
  }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val asArray = newArgs.take(1).map(_.asInstanceOf[Expression])
    val res = ST_MakePolygon(asArray.head)
    res.copyTagsFrom(this)
    res
  }

  }

  @ExpressionDescription(
  usage =
    "_FUNC_(expr1, expr2) - Creates a new Polygon geometry from: " +
      " a closed LineString representing the polygon boundary and" +
      " an Array of closed LineStrings representing holes in the polygon.",
  examples =
    """
    Examples:
      > SELECT _FUNC_(A, B);
  """,
  since = "1.0"
  )
case class ST_MakePolygonWithHoles(boundaryRing: Expression, holeRingArray: Expression)
  extends BinaryExpression
    with NullIntolerant
    with CodegenFallback {

  override def left: Expression = boundaryRing

  override def right: Expression = holeRingArray

  override def dataType: DataType = InternalGeometryType

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val ringGeom = InternalGeometry(
      boundaryRing.eval(input1.asInstanceOf[InternalRow]).asInstanceOf[InternalRow]
    )
    val holeGeoms = input2.asInstanceOf[ArrayData]
      .toObjectArray(InternalGeometryType)
      .map(g => InternalGeometry(g.asInstanceOf[InternalRow]))

    val polygon = new InternalGeometry(GeometryTypeEnum.POLYGON.id, ringGeom.boundaries, Array(holeGeoms.map(_.boundaries.head)))
    polygon.serialize
  }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
    val res = ST_MakePolygonWithHoles(asArray(0), asArray(1))
    res.copyTagsFrom(this)
    res
  }

  }