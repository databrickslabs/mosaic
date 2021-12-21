package com.databricks.mosaic.expressions.geometry

import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  UnaryExpression,
  NullIntolerant
}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType}
import com.databricks.mosaic.types.any2geometry
import org.locationtech.jts.io.ParseException

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
      case "MIN" => unitArray.reduceLeft(_ min _)
      case "MAX" => unitArray.reduceLeft(_ max _)
    }
  }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val geomArg = newArgs.head.asInstanceOf[Expression]
    val otherArgs = newArgs.tail.map(_.asInstanceOf[String])
    val res = ST_MinMaxXY(geomArg, otherArgs(0), otherArgs(1))
    res.copyTagsFrom(this)
    res
  }
}

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
      case e: ParseException           => return false
      case e: IllegalArgumentException => return false
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
