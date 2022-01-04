package com.databricks.mosaic.expressions.geometry

import com.databricks.mosaic.core.geometry.api.GeometryAPI
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}

case class ST_Centroid(inputGeom: Expression, geometryAPIName: String, nDim: Int = 2)
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
    val geometryAPI  = GeometryAPI(geometryAPIName)

    val geom = geometryAPI.geometry(input1, inputGeom.dataType)
    val centroid = geom.getCentroid
    nDim match {
      case 2 => InternalRow.fromSeq(Seq(centroid.getX, centroid.getY))
      case 3 => InternalRow.fromSeq(Seq(centroid.getX, centroid.getY, centroid.getZ))
    }
  }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val arg1 = newArgs.head.asInstanceOf[Expression]
    val res = ST_Centroid(arg1, geometryAPIName, nDim)
    res.copyTagsFrom(this)
    res
  }

}