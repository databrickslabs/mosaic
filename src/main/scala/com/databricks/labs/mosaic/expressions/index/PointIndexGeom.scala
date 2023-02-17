package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemFactory}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionInfo, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

case class PointIndexGeom(geom: Expression, resolution: Expression, indexSystem: IndexSystem, geometryAPIName: String)
    extends BinaryExpression
      with NullIntolerant
      with CodegenFallback {

    val geometryAPI: GeometryAPI = GeometryAPI(geometryAPIName)

    /** Expression output DataType. */
    override def dataType: DataType = indexSystem.getCellIdDataType

    /** Overridden to ensure [[Expression.sql]] is properly formatted. */
    override def prettyName: String = "grid_pointascellid"

    /**
      * Computes the index corresponding to the provided POINT geometry.
      *
      * @param input1
      *   Any instance containing a point geometry.
      * @param input2
      *   Any instance containing resolution.
      * @return
      *   Index id in Long.
      */
    override def nullSafeEval(input1: Any, input2: Any): Any = {
        val resolution: Int = indexSystem.getResolution(input2)

        // If another geometry type is provided, it will be converted to a centroid point.
        val point = geometryAPI.geometry(input1, geom.dataType).getCentroid
        val cellID = indexSystem.pointToIndex(point.getX, point.getY, resolution)

        indexSystem.serializeCellId(cellID)
    }

    override def toString: String = s"grid_pointascellid($geom, $resolution)"

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
        val res = PointIndexGeom(asArray(0), asArray(1), indexSystem, geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override def left: Expression = geom

    override def right: Expression = resolution

    override protected def withNewChildrenInternal(newLeft: Expression, newThird: Expression): Expression = copy(newLeft, newThird)

}

object PointIndexGeom {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[PointIndexGeom].getCanonicalName,
          db.orNull,
          "grid_pointascellid",
          """
            |    _FUNC_(geom, resolution) - Returns the index of a point geometry at resolution.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a, 10);
            |        622236721348804607
            |  """.stripMargin,
          "",
          "misc_funcs",
          "1.0",
          "",
          "built-in"
        )

}
