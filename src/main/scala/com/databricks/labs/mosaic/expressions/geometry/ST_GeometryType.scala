package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import java.util.Locale
import scala.util.{Success, Try}

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
              // TODO: code can be simplified if the function is registered and called 2 times
              val tryIO = Try {
                  ConvertToCodeGen.readGeometryCode(ctx, leftEval, inputGeom.dataType, geometryAPI)
              }

              // not merged into the same code block due to JTS IOException throwing
              // ESRI code will always remain simpler
              (tryIO, geometryAPI) match {
                  case (
                        Success((inCode, geomInRef)),
                        ESRI
                      ) => s"""
                              |$inCode
                              |${ev.value} = $javaStringClass.fromString($geomInRef.geometryType());
                              |""".stripMargin
                  case (
                        Success((inCode, geomInRef)),
                        JTS
                      ) => s"""
                              |try {
                              |$inCode
                              |${ev.value} = $javaStringClass.fromString($geomInRef.getGeometryType());
                              |} catch (Exception e) {
                              | throw e;
                              |}
                              |""".stripMargin
                  case _ => throw new IllegalArgumentException(s"Geometry API unsupported: $geometryAPIName.")
              }
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
