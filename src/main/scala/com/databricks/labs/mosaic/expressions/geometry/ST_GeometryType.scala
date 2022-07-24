package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import java.util.Locale

case class ST_GeometryType(inputGeom: Expression, geometryAPIName: String) extends UnaryExpression with NullIntolerant {

    /**
      * ST_GeometryType expression returns the OGC Geometry class name for a
      * given geometry, allowing basic type checking of geometries in more
      * complex functions.
      */

    override def child: Expression = inputGeom

    override def dataType: DataType = StringType

    override def nullSafeEval(input1: Any): Any = {
        val geometryAPI = GeometryAPI(geometryAPIName)

        val geom = geometryAPI.geometry(input1, inputGeom.dataType)
        UTF8String.fromString(geom.getGeometryType.toUpperCase(Locale.ROOT))
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(1).map(_.asInstanceOf[Expression])
        val res = ST_GeometryType(asArray(0), geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
        nullSafeCodeGen(
          ctx,
          ev,
          leftEval => {
              val geometryAPI = GeometryAPI.apply(geometryAPIName)
              val javaStringClass = CodeGenerator.javaType(StringType)
              val (inCode, geomInRef) = ConvertToCodeGen.readGeometryCode(ctx, leftEval, inputGeom.dataType, geometryAPI)
              val geometryTypeStatement = geometryAPI.geometryTypeCode
              geometryAPI.codeGenTryWrap(s"""
                                            |$inCode
                                            |${ev.value} = $javaStringClass.fromString($geomInRef.$geometryTypeStatement);
                                            |""".stripMargin)
          }
        )

    override protected def withNewChildInternal(newChild: Expression): Expression = copy(inputGeom = newChild)

}

object ST_GeometryType {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_GeometryType].getCanonicalName,
          db.orNull,
          "st_geometrytype",
          """
            |    _FUNC_(expr1) - Returns the Geometry class name for a given geometry.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a);
            |        {"MULTIPOLYGON"}
            |  """.stripMargin,
          "",
          "misc_funcs",
          "1.0",
          "",
          "built-in"
        )

}
