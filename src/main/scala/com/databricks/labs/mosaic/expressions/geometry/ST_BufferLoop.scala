package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, NullIntolerant, TernaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType

case class ST_BufferLoop(inputGeom: Expression, innerRadius: Expression, outerRadius: Expression, geometryAPIName: String)
    extends TernaryExpression
      with NullIntolerant {

    override def first: Expression = inputGeom

    override def second: Expression = innerRadius

    override def third: Expression = outerRadius

    override def dataType: DataType = inputGeom.dataType

    override def nullSafeEval(geomRow: Any, innerRadiusRow: Any, outerRadiusRow: Any): Any = {
        val geometryAPI = GeometryAPI(geometryAPIName)
        val geometry = geometryAPI.geometry(geomRow, inputGeom.dataType)
        val innerRadiusVal = innerRadiusRow.asInstanceOf[Double]
        val outerRadiusVal = outerRadiusRow.asInstanceOf[Double]
        val result = geometry.buffer(outerRadiusVal).difference(geometry.buffer(innerRadiusVal))
        geometryAPI.serialize(result, inputGeom.dataType)
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(3).map(_.asInstanceOf[Expression])
        val res = ST_BufferLoop(asArray.head, asArray(1), asArray(2), geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression =
        copy(newFirst, newSecond, newThird)

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
        nullSafeCodeGen(
          ctx,
          ev,
          (geomEval, r1, r2) => {
              val geometryAPI = GeometryAPI.apply(geometryAPIName)
              val result = ctx.freshName("result")
              val polygonClass = geometryAPI.geometryClass

              val (inCode, geomInRef) = ConvertToCodeGen.readGeometryCode(ctx, geomEval, inputGeom.dataType, geometryAPI)
              val (outCode, outGeomRef) = ConvertToCodeGen.writeGeometryCode(ctx, result, inputGeom.dataType, geometryAPI)

              geometryAPI.codeGenTryWrap(s"""
                                            |$inCode
                                            |$polygonClass $result = $geomInRef.buffer($r2).difference($geomInRef.buffer($r1));
                                            |$outCode
                                            |${ev.value} = $outGeomRef;
                                            |""".stripMargin)
          }
        )

}

object ST_BufferLoop {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_BufferLoop].getCanonicalName,
          db.orNull,
          "st_bufferloop",
          """
            |    _FUNC_(expr1) - Returns the buffer loop of the geometry.
            |    Buffer loop is the difference between the outer buffer and the inner buffer.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a, r1, r2);
            |        POLYGON(...) / MULTIPOLYGON(...)
            |  """.stripMargin,
          "",
          "misc_funcs",
          "1.0",
          "",
          "built-in"
        )

}
