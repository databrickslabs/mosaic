package com.databricks.labs.mosaic.expressions.geometry

import java.util.Locale

import com.databricks.labs.mosaic.core.crs.CRSBoundsProvider
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI

import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, NullIntolerant, TernaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{BooleanType, DataType}
import org.apache.spark.unsafe.types.UTF8String

case class ST_HasValidCoordinates(
    inputGeom: Expression,
    crsCode: Expression,
    which: Expression,
    geometryAPIName: String
) extends TernaryExpression
      with CodegenFallback
      with NullIntolerant {

    @transient
    val crsBoundsProvider: CRSBoundsProvider = CRSBoundsProvider(GeometryAPI(geometryAPIName))

    override def first: Expression = inputGeom

    override def second: Expression = crsCode

    override def third: Expression = which

    override def dataType: DataType = BooleanType

    override def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
        val geometryAPI = GeometryAPI(geometryAPIName)
        val geomIn = geometryAPI.geometry(input1, inputGeom.dataType)
        val crsCodeIn = input2.asInstanceOf[UTF8String].toString.split(":")
        val whichIn = input3.asInstanceOf[UTF8String].toString
        val crsBounds = whichIn.toLowerCase(Locale.ROOT) match {
            case "bounds"             => crsBoundsProvider.bounds(crsCodeIn(0), crsCodeIn(1).toInt)
            case "reprojected_bounds" => crsBoundsProvider.reprojectedBounds(crsCodeIn(0), crsCodeIn(1).toInt)
            case _ => throw new IllegalArgumentException("Only boundary and reprojected_boundary supported for which argument.")
        }
        (Seq(geomIn.getShellPoints) ++ geomIn.getHolePoints).flatten.flatten.forall(point =>
            crsBounds.lowerLeft.getX <= point.getX && point.getX <= crsBounds.upperRight.getX &&
            crsBounds.lowerLeft.getY <= point.getY && point.getY <= crsBounds.upperRight.getY
        )
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(3).map(_.asInstanceOf[Expression])
        val res = ST_HasValidCoordinates(asArray(0), asArray(1), asArray(2), geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression =
        copy(newFirst, newSecond, newThird)

}

object ST_HasValidCoordinates {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
            classOf[ST_HasValidCoordinates].getCanonicalName,
            db.orNull,
            "ST_HasValidCoordinates",
            """
              |    _FUNC_(expr1, expr2, expr3) - Checks if all points in geometry have
              |    valid coordinates with respect to provided crs code and
              |    the type of bounds.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(geom, 'EPSG:4326', 'bounds');
            |        true
            |  """.stripMargin,
          "",
          "misc_funcs",
          "1.0",
          "",
          "built-in"
        )
}
