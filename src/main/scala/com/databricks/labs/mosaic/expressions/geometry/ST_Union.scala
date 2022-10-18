package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionInfo, NullIntolerant}
import org.apache.spark.sql.types.DataType
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback

case class ST_Union(leftGeom: Expression, rightGeom: Expression, geometryAPIName: String)
    extends BinaryExpression
      with NullIntolerant
      with CodegenFallback {

    override def left: Expression = leftGeom

    override def right: Expression = rightGeom

    override def dataType: DataType = leftGeom.dataType

    override protected def nullSafeEval(leftGeom: Any, rightGeom: Any): Any = {
        val geometryAPI = GeometryAPI(geometryAPIName)
        val leftGeometry = geometryAPI.geometry(leftGeom, dataType)
        val rightGeometry = geometryAPI.geometry(rightGeom, dataType)
        val union = leftGeometry.union(rightGeometry)
        geometryAPI.serialize(union, dataType)
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
        val res = ST_Union(asArray(0), asArray(1), geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
        nullSafeCodeGen(
          ctx,
          ev,
          (leftEval, rightEval) => {
              val geometryAPI = GeometryAPI.apply(geometryAPIName)
              val union = ctx.freshName("union")
              val geometryClass = geometryAPI.geometryClass
              val (leftInCode, leftGeomInRef) = ConvertToCodeGen.readGeometryCode(ctx, leftEval, leftGeom.dataType, geometryAPI)
              val (rightInCode, rightGeomInRef) = ConvertToCodeGen.readGeometryCode(ctx, rightEval, rightGeom.dataType, geometryAPI)
              val (outCode, outGeomRef) = ConvertToCodeGen.writeGeometryCode(ctx, union, leftGeom.dataType, geometryAPI)
              geometryAPI.codeGenTryWrap(s"""
                                            |$leftInCode
                                            |$rightInCode
                                            |$geometryClass $union = $leftGeomInRef.union($rightGeomInRef);
                                            |$outCode
                                            |${ev.value} = $outGeomRef;
                                            |""".stripMargin)
          }
        )

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = copy(newLeft, newRight)

}

object ST_Union {
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_Union].getCanonicalName,
          db.orNull,
          "st_union",
          """
            |    _FUNC_(expr1, expr2) - Returns the union of the input geometries.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a, b);
            |        {"POLYGON (( ... ))"}
            |  """.stripMargin,
          "",
          "misc_funcs",
          "1.0",
          "",
          "built-in"
        )
}
