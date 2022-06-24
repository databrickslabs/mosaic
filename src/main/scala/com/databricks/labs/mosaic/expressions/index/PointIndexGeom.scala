package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint
import com.databricks.labs.mosaic.core.index.{H3IndexSystem, IndexSystemID}
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum
import com.databricks.labs.mosaic.sql.MosaicSQLExceptions

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionInfo, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

case class PointIndexGeom(geom: Expression, resolution: Expression, indexSystemName: String, geometryAPIName: String)
    extends BinaryExpression
      with NullIntolerant
      with CodegenFallback {

    /** Expression output DataType. */
    override def dataType: DataType = LongType

    /** Overridden to ensure [[Expression.sql]] is properly formatted. */
    override def prettyName: String = "point_index_geom"

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
        val indexSystem = IndexSystemID.getIndexSystem(IndexSystemID(indexSystemName))
        val resolution: Int = indexSystem.getResolution(input2)
        val geometryAPI = GeometryAPI(geometryAPIName)
        val rowGeom = geometryAPI.geometry(input1, geom.dataType)
        val geomType = GeometryTypeEnum.fromString(rowGeom.getGeometryType)
        geomType match {
            case GeometryTypeEnum.POINT =>
                val point = rowGeom.asInstanceOf[MosaicPoint]
                indexSystem.pointToIndex(point.getX, point.getY, resolution)
            case _ => throw MosaicSQLExceptions.IncorrectGeometryTypeSupplied(toString, geomType, GeometryTypeEnum.POINT)
        }
    }

    override def toString: String = s"point_index_geom($geom, $resolution)"

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
        val res = PointIndexGeom(asArray(0), asArray(1), indexSystemName, geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override def left: Expression = geom

    override def right: Expression = resolution

    override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression): Expression =
        copy(geom = newFirst, resolution = newSecond)

}

object PointIndexGeom {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[PointIndexGeom].getCanonicalName,
          db.orNull,
          "point_index_geom",
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
