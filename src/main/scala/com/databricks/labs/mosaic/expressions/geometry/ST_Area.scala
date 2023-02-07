package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.api._
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{DataType, DoubleType}

case class ST_Area(inputGeom: Expression, geometryAPIName: String) extends UnaryExpression with NullIntolerant {

    /**
      * ST_Area expression returns area covered by the
      * [[org.locationtech.jts.geom.Geometry]] instance extracted from inputGeom
      * expression.
      */

    override def child: Expression = inputGeom

    /** Output Data Type */
    override def dataType: DataType = DoubleType

    override def nullSafeEval(input1: Any): Any = {
        val geometryAPI = GeometryAPI(geometryAPIName)
        val geom = geometryAPI.geometry(input1, inputGeom.dataType)
        geom.getArea
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(1).map(_.asInstanceOf[Expression])
        val res = ST_Area(asArray(0), geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
        nullSafeCodeGen(
          ctx,
          ev,
          eval => {
              val geometryAPI = GeometryAPI.apply(geometryAPIName)
              val (inCode, geomInRef) = ConvertToCodeGen.readGeometryCode(ctx, eval, inputGeom.dataType, geometryAPI)
              val areaStatement = geometryAPI.geometryAreaCode
              geometryAPI.codeGenTryWrap(s"""
                                            |$inCode
                                            |${ev.value} = $geomInRef.$areaStatement;
                                            |""".stripMargin)
          }
        )

    override protected def withNewChildInternal(newChild: Expression): Expression = copy(inputGeom = newChild)

}

object ST_Area {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_Area].getCanonicalName,
          db.orNull,
          "st_area",
          """
            |    _FUNC_(expr1) - Returns the area of the geometry.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a);
            |        15.2512
            |  """.stripMargin,
          "",
          "misc_funcs",
          "1.0",
          "",
          "built-in"
        )

}
