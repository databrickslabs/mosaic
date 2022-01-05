package com.databricks.mosaic.expressions.geometry

import com.databricks.mosaic.core.geometry.api.GeometryAPI
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.types.{DataType, DoubleType}

case class ST_MinMaxXYZ(inputGeom: Expression, geometryAPIName: String, dimension: String, func: String)
  extends UnaryExpression
    with NullIntolerant
    with CodegenFallback {

  override def child: Expression = inputGeom

  override def dataType: DataType = DoubleType

  override def nullSafeEval(input1: Any): Any = {
    val geometryAPI = GeometryAPI(geometryAPIName)

    val geom = geometryAPI.geometry(input1, inputGeom.dataType)
    val coordArray = geom.getBoundary
    val unitArray = dimension match {
      case "X" => coordArray.map(_.getX)
      case "Y" => coordArray.map(_.getY)
      case "Z" => coordArray.map(_.getZ)
    }
    func match {
      case "MIN" => unitArray.min
      case "MAX" => unitArray.max
    }
  }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val geomArg = newArgs.head.asInstanceOf[Expression]
    val res = ST_MinMaxXYZ(geomArg, geometryAPIName, dimension, func)
    res.copyTagsFrom(this)
    res
  }
}
