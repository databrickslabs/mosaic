package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import org.apache.spark.sql.catalyst.expressions.UnaryExpression
import org.apache.spark.sql.catalyst.expressions.NullIntolerant
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.DataType
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo

case class ST_UnaryUnion(inputGeom: Expression, geometryAPIName: String) extends UnaryExpression with NullIntolerant with CodegenFallback {

    override def child: Expression = inputGeom

    override def dataType: DataType = inputGeom.dataType

    override protected def nullSafeEval(input: Any): Any = {
        val geometryAPI = GeometryAPI(geometryAPIName)
        val geometry = geometryAPI.geometry(input, inputGeom.dataType)
        val unaryUnion = geometry.unaryUnion
        geometryAPI.serialize(unaryUnion, inputGeom.dataType)
    }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val asArray = newArgs.take(1).map(_.asInstanceOf[Expression])
    val res = ST_UnaryUnion(asArray.head, geometryAPIName)
    res.copyTagsFrom(this)
    res
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    nullSafeCodeGen(
      ctx,
      ev,
      leftEval => {
        val geometryAPI = GeometryAPI.apply(geometryAPIName)
        val unaryUnion = ctx.freshName("unaryUnion")
        val geometryClass = geometryAPI.geometryClass
        val (inCode, geomInRef) = ConvertToCodeGen.readGeometryCode(ctx, leftEval, inputGeom.dataType, geometryAPI)
        val (outCode, outGeomRef) = ConvertToCodeGen.writeGeometryCode(ctx, unaryUnion, inputGeom.dataType, geometryAPI)

        geometryAPI.name match {
          case "ESRI" => geometryAPI.codeGenTryWrap (s"""
                          |$inCode
                          |$geometryClass $unaryUnion = $geomInRef.union($geomInRef);
                          |$outCode
                          |${ev.value} = $outGeomRef;
                          |""".stripMargin)
          case "JTS" => geometryAPI.codeGenTryWrap(s"""
                          |$inCode
                          |$geometryClass $unaryUnion = $geomInRef.union();
                          |$outCode
                          |${ev.value} = $outGeomRef;
                          |""".stripMargin)
        }
      }
    )

    override protected def withNewChildInternal(newChild: Expression): Expression = copy(inputGeom = newChild)

}

object ST_UnaryUnion {
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_UnaryUnion].getCanonicalName,
          db.orNull,
          "st_unaryunion",
          """
            |    _FUNC_(expr1) - Returns the union of the input geometry.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a);
            |        {"POLYGON (( 0 0, 1 0, 1 1, 0 1 ))"}
            |  """.stripMargin,
          "",
          "misc_funcs",
          "1.0",
          "",
          "built-in"
        )
}
