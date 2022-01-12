package com.databricks.mosaic.expressions.geometry

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType

import com.databricks.mosaic.core.geometry.api.GeometryAPI
@ExpressionDescription(
  usage = "_FUNC_(expr1, td) - Returns a new geometry rotated by td radians.",
  examples = """
    Examples:
      > SELECT _FUNC_(a, td);
       POLYGON ((...))
               """,
  since = "1.0"
)
case class ST_Rotate(inputGeom: Expression, td: Expression, geometryAPIName: String)
    extends BinaryExpression
      with NullIntolerant
      with CodegenFallback {

    /**
      * ST_Rotate expression returns are covered by the
      * [[org.locationtech.jts.geom.Geometry]] instance extracted from inputGeom
      * expression.
      */

    // noinspection DuplicatedCode
    override def nullSafeEval(input1: Any, input2: Any): Any = {
        val geometryAPI = GeometryAPI(geometryAPIName)
        val geom = geometryAPI.geometry(input1, dataType)
        val tDist = input2.asInstanceOf[Double]
        val result = geom.rotate(tDist)
        geometryAPI.serialize(result, dataType)
    }

    /** Output Data Type */
    override def dataType: DataType = inputGeom.dataType

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
        val res = ST_Rotate(asArray(0), asArray(1), geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override def left: Expression = inputGeom

    override def right: Expression = td

}
