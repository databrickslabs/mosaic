package com.databricks.mosaic.expressions.geometry

import com.databricks.mosaic.core.types.any2geometry
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}

case class ST_Centroid(inputGeom: Expression, nDim: Int = 2)
  extends UnaryExpression
    with NullIntolerant
    with CodegenFallback {

  /**
   * ST_Centroid expression returns the centroid of
   * the [[org.locationtech.jts.geom.Geometry]]
   * instance extracted from inputGeom expression.
   */

  override def child: Expression = inputGeom

  /** Output Data Type */
  override def dataType: DataType = nDim match {
    case 2 => StructType(Seq(StructField("x", DoubleType), StructField("y", DoubleType)))
    case 3 => StructType(Seq(StructField("x", DoubleType), StructField("y", DoubleType), StructField("z", DoubleType)))
    case _ => throw new NotImplementedError("Only 2D and 3D centroid supported!")
  }

  override def nullSafeEval(input1: Any): Any = {
    val geom = any2geometry(input1, inputGeom.dataType)
    val coord = geom.getCentroid.getCoordinate
    nDim match {
      case 2 => InternalRow.fromSeq(Seq(coord.x, coord.y))
      case 3 => InternalRow.fromSeq(Seq(coord.x, coord.y, coord.z))
    }
  }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val arg1 = newArgs.head.asInstanceOf[Expression]
    val res = ST_Centroid(arg1, nDim)
    res.copyTagsFrom(this)
    res
  }

}