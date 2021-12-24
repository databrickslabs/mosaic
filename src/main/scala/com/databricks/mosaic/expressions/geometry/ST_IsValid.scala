package com.databricks.mosaic.expressions.geometry

import org.locationtech.jts.io.ParseException

import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{BooleanType, DataType}

import com.databricks.mosaic.core.types.any2geometry

case class ST_IsValid(inputGeom: Expression)
  extends UnaryExpression
    with NullIntolerant
    with CodegenFallback {

  override def child: Expression = inputGeom

  override def dataType: DataType = BooleanType

  override def nullSafeEval(input1: Any): Any = {
    try {
      val geom = any2geometry(input1, inputGeom.dataType)
      return geom.isValid
    } catch {
      case _: ParseException => return false
      case _: IllegalArgumentException => return false
    }
    false
  }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val asArray = newArgs.take(1).map(_.asInstanceOf[Expression])
    val res = ST_IsValid(asArray(0))
    res.copyTagsFrom(this)
    res
  }
}
