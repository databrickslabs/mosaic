package com.databricks.mosaic.expressions.index

import com.databricks.mosaic.core.geometry.api.GeometryAPI
import com.databricks.mosaic.core.geometry.point.MosaicPoint
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionInfo, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._
import com.databricks.mosaic.core.index.{H3IndexSystem, IndexSystemID}
import com.databricks.mosaic.core.types.model.GeometryTypeEnum

case class PointIndex(geom: Expression, resolution: Expression, indexSystemName: String, geometryAPIName: String)
    extends BinaryExpression
        with NullIntolerant
        with CodegenFallback {

    /** Expression output DataType. */
    override def dataType: DataType = LongType

    override def toString: String = s"point_index($geom, $resolution)"

    /** Overridden to ensure [[Expression.sql]] is properly formatted. */
    override def prettyName: String = "point_index"

    /**
     * Computes the H3 index corresponding to the provided lat and long
     * coordinates.
     *
     * @param input1
     *   Any instance containing a point geometry.
     * @param input2
     *   Any instance containing resolution.
     * @return
     *   H3 index id in Long.
     */
    override def nullSafeEval(input1: Any, input2: Any): Any = {
        val resolution: Int = H3IndexSystem.getResolution(input2)
        val geometryAPI = GeometryAPI(geometryAPIName)
        val rowGeom = geometryAPI.geometry(input1, geom.dataType)
        val geomType = GeometryTypeEnum.fromString(rowGeom.getGeometryType)
        val indexSystem = IndexSystemID.getIndexSystem(IndexSystemID(indexSystemName))
        geomType match {
            case GeometryTypeEnum.POINT =>
                val point = rowGeom.asInstanceOf[MosaicPoint]
                indexSystem.pointToIndex(point.getX, point.getY, resolution)
            case _ =>
        }
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
        val res = PointIndex(asArray(0), asArray(1), indexSystemName, geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression): Expression =
        copy(geom = newFirst, resolution = newSecond)

    override def left: Expression = geom

    override def right: Expression = resolution
}


object PointIndex {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
            classOf[PointIndex].getCanonicalName,
            db.orNull,
            "point_index",
            """
              |    _FUNC_(geom, resolution) - Returns the h3 index of a point geometry at resolution.
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