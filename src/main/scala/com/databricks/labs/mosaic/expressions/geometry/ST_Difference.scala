package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionInfo, NullIntolerant}
import org.apache.spark.sql.types.DataType
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}

case class ST_Difference(leftGeom: Expression, rightGeom: Expression, geometryAPIName: String)
    extends BinaryExpression
        with NullIntolerant {

    override def left: Expression = leftGeom

    override def right: Expression = rightGeom

    override def dataType: DataType = leftGeom.dataType

    override protected def nullSafeEval(leftGeom: Any, rightGeom: Any): Any = {
        val geometryAPI = GeometryAPI(geometryAPIName)
        val leftGeometry = geometryAPI.geometry(leftGeom, dataType)
        val rightGeometry = geometryAPI.geometry(rightGeom, dataType)
        val difference = leftGeometry.difference(rightGeometry)
        geometryAPI.serialize(difference, dataType)
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
        val res = ST_Difference(asArray(0), asArray(1), geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
        nullSafeCodeGen(
            ctx,
            ev,
            (leftEval, rightEval) => {
                val geometryAPI = GeometryAPI.apply(geometryAPIName)
                val difference = ctx.freshName("difference")
                val geometryClass = geometryAPI.geometryClass
                val (leftInCode, leftGeomInRef) = ConvertToCodeGen.readGeometryCode(ctx, leftEval, leftGeom.dataType, geometryAPI)
                val (rightInCode, rightGeomInRef) = ConvertToCodeGen.readGeometryCode(ctx, rightEval, rightGeom.dataType, geometryAPI)
                val (outCode, outGeomRef) = ConvertToCodeGen.writeGeometryCode(ctx, difference, leftGeom.dataType, geometryAPI)
                geometryAPI.codeGenTryWrap(s"""
                                              |$leftInCode
                                              |$rightInCode
                                              |$geometryClass $difference = $leftGeomInRef.difference($rightGeomInRef);
                                              |$outCode
                                              |${ev.value} = $outGeomRef;
                                              |""".stripMargin)
            }
        )

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = copy(newLeft, newRight)

}

object ST_Difference {
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
            classOf[ST_Difference].getCanonicalName,
            db.orNull,
            "st_difference",
            """
              |    _FUNC_(expr1, expr2) - Returns the difference of the two geometries.
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
