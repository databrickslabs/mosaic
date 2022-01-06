package com.databricks.mosaic.expressions.constructors

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType

import com.databricks.mosaic.core.types.InternalGeometryType
import com.databricks.mosaic.core.types.model.{GeometryTypeEnum, InternalGeometry}


@ExpressionDescription(
  usage =
    "_FUNC_(expr1) - Creates a new LineString geometry from an Array of Point, MultiPoint, LineString or MultiLineString geometries.",
  examples =
    """
    Examples:
      > SELECT _FUNC_(A);
  """,
  since = "1.0"
  )
case class ST_MakeLine(geoms: Expression)
  extends UnaryExpression
    with NullIntolerant
    with CodegenFallback {

  override def child: Expression = geoms

  override def dataType: DataType = InternalGeometryType

  def reduceGeoms(leftGeom: InternalGeometry, rightGeom: InternalGeometry): InternalGeometry =
    new InternalGeometry(
      GeometryTypeEnum.LINESTRING.id,
      Array(leftGeom.boundaries.flatMap(_.toList) ++ rightGeom.boundaries.flatMap(_.toList)),
      Array(Array())
      )

  override def eval(input: InternalRow): Any = {
    val geomArray = geoms.eval(input).asInstanceOf[ArrayData].toObjectArray(InternalGeometryType)
    val internalGeoms = geomArray.map(g => InternalGeometry(g.asInstanceOf[InternalRow]))
    val outputGeom = internalGeoms.reduce(reduceGeoms)
    outputGeom.serialize
    
  }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val asArray = newArgs.take(1).map(_.asInstanceOf[Expression])
    val res = ST_MakeLine(asArray.head)
    res.copyTagsFrom(this)
    res
  }
  
}
