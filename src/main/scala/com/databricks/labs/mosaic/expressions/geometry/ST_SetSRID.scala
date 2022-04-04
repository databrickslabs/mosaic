package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionInfo, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

case class ST_SetSRID(inputGeom: Expression, srid: Expression, geometryAPIName: String)
    extends BinaryExpression
      with NullIntolerant
      with CodegenFallback
      with RequiresCRS {

    override def nullSafeEval(input1: Any, input2: Any): Any = {
        checkEncoding(inputGeom.dataType)
        val geometryAPI = GeometryAPI(geometryAPIName)
        val geom = geometryAPI.geometry(input1, inputGeom.dataType)
        geom.setSpatialReference(input2.asInstanceOf[Int])
        geometryAPI.serialize(geom, dataType)
    }

    /** Output Data Type */
    override def dataType: DataType = inputGeom.dataType

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
        val res = ST_SetSRID(asArray(0), asArray(1), geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override def left: Expression = inputGeom

    override def right: Expression = srid

//    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
//        nullSafeCodeGen(
//            ctx,
//            ev,
//            leftEval => {
//                checkEncoding(inputGeom.dataType)
//                val geometryAPI = GeometryAPI.apply(geometryAPIName)
//                val (inCode, geomInRef) = ConvertToCodeGen.readGeometryCode(ctx, leftEval, inputGeom.dataType, geometryAPI)
//
//                geometryAPIName match {
//                    case "ESRI" => s"""
//                                      |$inCode
//                                      |${ev.value} = $geomInRef.setEsriSpatialReference().getID();
//                                      |""".stripMargin
//                    case "JTS" => s"""
//                                     |try {
//                                     |$inCode
//                                     |${ev.value} = $geomInRef.setSRID($);
//                                     |} catch (Exception e) {
//                                     | throw e;
//                                     |}
//                                     |""".stripMargin
//
//                }
//            }
//        )

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
        copy(inputGeom = newLeft, srid = newRight)

}

object ST_SetSRID {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_SetSRID].getCanonicalName,
          db.orNull,
          "ST_SetSRID",
          """
            |    _FUNC_(expr1) - Assigns a spatial reference system to the geometry.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a);
            |        POINT (1 1)
            |  """.stripMargin,
          "",
          "misc_funcs",
          "1.0",
          "",
          "built-in"
        )
}
