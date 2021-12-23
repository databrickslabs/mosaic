package com.databricks.mosaic.expressions.geometry

import com.databricks.mosaic.core.types.any2geometry
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.types.{DataType, DoubleType}

case class ST_MinMaxXY(inputGeom: Expression, dimension: String, func: String)
    extends UnaryExpression
    with NullIntolerant
    with CodegenFallback {

  override def child: Expression = inputGeom

  override def dataType: DataType = DoubleType

  override def nullSafeEval(input1: Any): Any = {
    val geom = any2geometry(input1, inputGeom.dataType)
    val coordArray = geom.getCoordinates
    val unitArray = dimension match {
      case "X" => coordArray.map(_.x)
      case "Y" => coordArray.map(_.y)
    }
    func match {
      case "MIN" => unitArray.min
      case "MAX" => unitArray.max
    }
  }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val geomArg = newArgs.head.asInstanceOf[Expression]
    val res = ST_MinMaxXY(geomArg, dimension, func)
    res.copyTagsFrom(this)
    res
  }
}
