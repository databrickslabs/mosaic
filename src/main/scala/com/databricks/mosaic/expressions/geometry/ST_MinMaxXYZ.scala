package com.databricks.mosaic.expressions.geometry

import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{DataType, DoubleType}

import com.databricks.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.mosaic.core.geometry.{MosaicGeometryJTS, MosaicGeometryOGC}
import com.databricks.mosaic.core.geometry.api.GeometryAPI

case class ST_MinMaxXYZ(inputGeom: Expression, geometryAPIName: String, dimension: String, func: String)
    extends UnaryExpression
      with NullIntolerant {

    override def child: Expression = inputGeom

    override def dataType: DataType = DoubleType

    override def nullSafeEval(input1: Any): Any = {
        val geometryAPI = GeometryAPI(geometryAPIName)

        val geom = geometryAPI.geometry(input1, inputGeom.dataType)
        geom.minMaxCoord(dimension, func)
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val geomArg = newArgs.head.asInstanceOf[Expression]
        val res = ST_MinMaxXYZ(geomArg, geometryAPIName, dimension, func)
        res.copyTagsFrom(this)
        res
    }

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
        nullSafeCodeGen(
            ctx,
            ev,
            leftEval => {
                val geometryAPI = GeometryAPI.apply(geometryAPIName)
                val (inCode, geomInRef) = ConvertToCodeGen.readGeometryCode(ctx, leftEval, inputGeom.dataType, geometryAPI)
                val mosaicGeometryOGC = classOf[MosaicGeometryOGC].getName
                val mosaicGeometryJTS = classOf[MosaicGeometryJTS].getName

                geometryAPIName match {
                    case "OGC" => s"""
                                     |$inCode
                                     |${ev.value} = $mosaicGeometryOGC.apply($geomInRef).minMaxCoord("$dimension", "$func");
                                     |""".stripMargin
                    case "JTS" => s"""
                                     |try {
                                     |$inCode
                                     |${ev.value} = $mosaicGeometryJTS.apply($geomInRef).minMaxCoord("$dimension", "$func");
                                     |} catch (Exception e) {
                                     | throw e;
                                     |}
                                     |""".stripMargin

                }
            }
        )

    override protected def withNewChildInternal(newChild: Expression): Expression = copy(inputGeom = newChild)
}

object ST_MinMaxXYZ {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String], name: String): ExpressionInfo =
        new ExpressionInfo(
            classOf[ST_Length].getCanonicalName,
            db.orNull,
            name,
            """
              |    _FUNC_(expr1) - Returns min/max coord for a given geometry.
            """.stripMargin,
            "",
            """
              |    Examples:
              |      > SELECT _FUNC_(a);
              |        13.23
              |  """.stripMargin,
            "",
            "misc_funcs",
            "1.0",
            "",
            "built-in"
        )
}
