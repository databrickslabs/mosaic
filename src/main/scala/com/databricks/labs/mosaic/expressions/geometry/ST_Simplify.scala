package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.MosaicGeometryJTS
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionInfo, NullIntolerant}
import org.apache.spark.sql.types.DataType
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.locationtech.jts.geom.Geometry

case class ST_Simplify(inputGeom: Expression, tolerance: Expression, geometryAPIName: String) extends BinaryExpression with NullIntolerant {

    override def left: Expression = inputGeom

    override def right: Expression = tolerance

    override def dataType: DataType = inputGeom.dataType

    override protected def nullSafeEval(geom: Any, tol: Any): Any = {
        val geometryAPI = GeometryAPI(geometryAPIName)
        val geometry = geometryAPI.geometry(geom, inputGeom.dataType)
        val simplify = geometry.simplify(tol.asInstanceOf[Double])
        geometryAPI.serialize(simplify, inputGeom.dataType)
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
        val res = ST_Simplify(asArray(0), asArray(1), geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
        nullSafeCodeGen(
          ctx,
          ev,
          (leftEval, rightEval) => {
              val geometryAPI = GeometryAPI.apply(geometryAPIName)
              val simplified = ctx.freshName("simplified")
              val GeometryClass = geometryAPI.geometryClass

              val (inCode, geomInRef) = ConvertToCodeGen.readGeometryCode(ctx, leftEval, inputGeom.dataType, geometryAPI)
              val (outCode, outGeomRef) = ConvertToCodeGen.writeGeometryCode(ctx, simplified, inputGeom.dataType, geometryAPI)

              geometryAPI.name match {
                  case "ESRI" => geometryAPI.codeGenTryWrap(s"""
                                                               |$inCode
                                                               |$GeometryClass $simplified = $geomInRef.makeSimple();
                                                               |$outCode
                                                               |${ev.value} = $outGeomRef;
                                                               |""".stripMargin)
                  case "JTS"  =>
                      val jtsGeometryClass = classOf[Geometry].getName
                      val mosaicGeometryJTSClass = classOf[MosaicGeometryJTS].getName
                      geometryAPI.codeGenTryWrap(s"""
                                                    |$inCode
                                                    |$jtsGeometryClass $simplified = (($mosaicGeometryJTSClass)$mosaicGeometryJTSClass.apply($geomInRef).simplify($rightEval)).getGeom();
                                                    |$outCode
                                                    |${ev.value} = $outGeomRef;
                                                    |""".stripMargin)

              }
          }
        )

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
        copy(inputGeom = newLeft, tolerance = newRight)

}

object ST_Simplify {
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_Simplify].getCanonicalName,
          db.orNull,
          "st_simplify",
          """
            |    _FUNC_(expr1) - Returns the simplified input geometry.
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
