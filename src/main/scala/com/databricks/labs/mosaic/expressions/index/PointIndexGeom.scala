package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint
import com.databricks.labs.mosaic.core.index.IndexSystemID
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum
import com.databricks.labs.mosaic.sql.MosaicSQLExceptions
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, Literal, NullIntolerant, TernaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class PointIndexGeom(geom: Expression, resolution: Expression, idAsLong: Expression, indexSystemName: String, geometryAPIName: String)
    extends TernaryExpression
      with NullIntolerant
      with CodegenFallback {

    /** Expression output DataType. */
    override def dataType: DataType =
        idAsLong match {
            case Literal(f: Boolean, BooleanType) => if (f) LongType else StringType
            case _                                => throw new Error("idAsLong has to be Boolean type.")
        }

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
    override def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
        val indexSystem = IndexSystemID.getIndexSystem(IndexSystemID(indexSystemName))
        val resolution: Int = indexSystem.getResolution(input2)
        val idAsLongVal = input3.asInstanceOf[Boolean]
        val geometryAPI = GeometryAPI(geometryAPIName)
        val rowGeom = geometryAPI.geometry(input1, geom.dataType)
        val geomType = GeometryTypeEnum.fromString(rowGeom.getGeometryType)
        val cellID = geomType match {
            case GeometryTypeEnum.POINT =>
                val point = rowGeom.asInstanceOf[MosaicPoint]
                indexSystem.pointToIndex(point.getX, point.getY, resolution)
            case _ => throw MosaicSQLExceptions.IncorrectGeometryTypeSupplied(toString, geomType, GeometryTypeEnum.POINT)
        }
        if (idAsLongVal) cellID else UTF8String.fromString(indexSystem.format(cellID))
    }

    override def toString: String = s"grid_pointascellid($geom, $resolution)"

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(3).map(_.asInstanceOf[Expression])
        val res = PointIndexGeom(asArray(0), asArray(1), asArray(2), indexSystemName, geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override def first: Expression = geom

    override def second: Expression = resolution

    override def third: Expression = idAsLong

    override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression =
        copy(newFirst, newSecond, newThird)

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
