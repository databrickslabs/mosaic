package com.databricks.mosaic.expressions.geometry

import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType

import com.databricks.mosaic.core.geometry.api.GeometryAPI
import com.databricks.mosaic.core.geometry.multipoint.MosaicMultiPoint
import com.databricks.mosaic.core.types.InternalGeometryType

@ExpressionDescription(
  usage = "_FUNC_(expr1) - Returns the convex hull for a given MultiPoint geometry.",
  examples = """
    Examples:
      > SELECT _FUNC_(a);
       {"POLYGON (( 0 0, 1 0, 1 1, 0 1 ))"}
  """,
  since = "1.0"
)
case class ST_ConvexHull(inputGeom: Expression, geometryAPIName: String) extends UnaryExpression with NullIntolerant with CodegenFallback {

    override def child: Expression = inputGeom

    override def dataType: DataType = InternalGeometryType

    override def nullSafeEval(inputRow: Any): Any = {
        val geometryAPI = GeometryAPI(geometryAPIName)
        val geometry = geometryAPI.geometry(inputRow, inputGeom.dataType)
        if (geometry.getGeometryType != "MultiPoint") {
            throw new IllegalArgumentException(s"Geometry provided to ST_ConvexHull must be MultiPoint (was ${geometry.getGeometryType}).")
        }
        val convexHull = geometry.asInstanceOf[MosaicMultiPoint].convexHull

        convexHull.toInternal.serialize
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(1).map(_.asInstanceOf[Expression])
        val res = ST_ConvexHull(asArray.head, geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

}
