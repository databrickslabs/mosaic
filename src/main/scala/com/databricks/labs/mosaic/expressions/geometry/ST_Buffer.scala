package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionInfo, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType

case class ST_Buffer(inputGeom: Expression, radius: Expression, geometryAPIName: String) extends BinaryExpression with NullIntolerant {

    override def left: Expression = inputGeom

    override def right: Expression = radius

    override def dataType: DataType = inputGeom.dataType

    override def nullSafeEval(geomRow: Any, radiusRow: Any): Any = {
        val geometryAPI = GeometryAPI(geometryAPIName)
        val geometry = geometryAPI.geometry(geomRow, inputGeom.dataType)
        val radiusVal = radiusRow.asInstanceOf[Double]
        val buffered = geometry.buffer(radiusVal)
        geometryAPI.serialize(buffered, inputGeom.dataType)
    }

//    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
//        val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
//        val res = ST_Buffer(asArray.head, asArray(1), geometryAPIName)
//        res.copyTagsFrom(this)
//        res
//    }

    override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = copy(newLeft, newRight)

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
        nullSafeCodeGen(
          ctx,
          ev,
          (leftEval, rightEval) => {
              val geometryAPI = GeometryAPI.apply(geometryAPIName)
              val buffered = ctx.freshName("buffered")
              val polygonClass = geometryAPI.geometryClass

              val (inCode, geomInRef) = ConvertToCodeGen.readGeometryCode(ctx, leftEval, inputGeom.dataType, geometryAPI)
              val (outCode, outGeomRef) = ConvertToCodeGen.writeGeometryCode(ctx, buffered, inputGeom.dataType, geometryAPI)

              geometryAPI.codeGenTryWrap(s"""
                                            |$inCode
                                            |$polygonClass $buffered = $geomInRef.buffer($rightEval);
                                            |$outCode
                                            |${ev.value} = $outGeomRef;
                                            |""".stripMargin)
          }
        )

}

object ST_Buffer {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_Buffer].getCanonicalName,
          db.orNull,
          "st_buffer",
          """
            |    _FUNC_(expr1, expr2) - Returns expr1 buffered by expr2.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a, b);
            |        POLYGON((1 1, 2 2, 3 3 ....))
            |  """.stripMargin,
          "",
          "misc_funcs",
          "1.0",
          "",
          "built-in"
        )

}
