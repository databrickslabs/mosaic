package com.databricks.mosaic.expressions.constructors

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{DataType, DoubleType}

import com.databricks.mosaic.core.types.InternalGeometryType
import com.databricks.mosaic.core.types.model.{GeometryTypeEnum, InternalCoord, InternalGeometry}
import scala.collection.mutable.ArrayBuffer


@ExpressionDescription(
  usage =
    "_FUNC_(expr1) - Creates a new Polygon geometry from an Array of length-2 Arrays representing the polygon boundary.",
  examples =
    """
    Examples:
      > SELECT _FUNC_(A);
  """,
  since = "1.0"
  )
case class ST_Polygon(boundaryRingArray: Expression)
  extends UnaryExpression
    with NullIntolerant
    with CodegenFallback {
  override def child: Expression = boundaryRingArray

  override def dataType: DataType = InternalGeometryType

  private lazy val coordArray = new ArrayBuffer[InternalCoord]

  override def eval(input: InternalRow): Any = {
    val boundaryRing = boundaryRingArray.eval(input).asInstanceOf[ArrayData]
    
    val numCoords = boundaryRing.numElements()
    var i = 0
    while (i < numCoords) {
      val coord = boundaryRing.getArray(i)
      coordArray += new InternalCoord(Seq(coord.getDouble(0), coord.getDouble(1)))
      i += 1
    } 
    val boundary = coordArray.toArray
    val polygon = new InternalGeometry(GeometryTypeEnum.POLYGON.id, Array(boundary), Array(Array()))
    polygon.serialize
  }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val asArray = newArgs.take(1).map(_.asInstanceOf[Expression])
    val res = ST_Polygon(asArray.head)
    res.copyTagsFrom(this)
    res
  }

  }

  @ExpressionDescription(
  usage =
    "_FUNC_(expr1, expr2) - Creates a new Polygon geometry from: " +
      " an Array of length-2 Arrays representing the polygon boundary; and" +
      " an Array of Arrays of length-2 Arrays representing holes in the polygon.",
  examples =
    """
    Examples:
      > SELECT _FUNC_(A, B);
  """,
  since = "1.0"
  )
case class ST_PolygonWithHoles(boundaryRingArray: Expression, holeRingArray: Expression)
  extends BinaryExpression
    with NullIntolerant
    with CodegenFallback {

  override def left: Expression = boundaryRingArray

  override def right: Expression = holeRingArray

  override def dataType: DataType = InternalGeometryType

  private lazy val coordArray = new ArrayBuffer[InternalCoord]

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val boundaryRing = input1.asInstanceOf[ArrayData]
    val holeRings = input2.asInstanceOf[ArrayData]
    
    val numCoords = boundaryRing.numElements()
    var i = 0
    while (i < numCoords) {
      val coord = boundaryRing.getArray(i)
      coordArray += new InternalCoord(Seq(coord.getDouble(0), coord.getDouble(1)))
      i += 1
    } 
    val boundary = coordArray.toArray

    val polygon = new InternalGeometry(GeometryTypeEnum.POLYGON.id, Array(boundary), Array(Array()))
    polygon.serialize
  }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
    val res = ST_PolygonWithHoles(asArray(0), asArray(1))
    res.copyTagsFrom(this)
    res
  }

  }