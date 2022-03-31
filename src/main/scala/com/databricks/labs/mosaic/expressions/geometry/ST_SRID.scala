package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI

import org.apache.spark.sql.catalyst.expressions.{
    Attribute,
    AttributeReference,
    Expression,
    ExpressionInfo,
    NamedExpression,
    NullIntolerant,
    UnaryExpression
}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, Metadata}

case class ST_SRID(inputGeom: Expression, geometryAPIName: String) extends UnaryExpression with NullIntolerant {

    def dataType: DataType = IntegerType

    def child: Expression = inputGeom

    override def nullSafeEval(input1: Any): Any = {
        val attr = new AttributeReference(
          inputGeom.nodeName,
          inputGeom.dataType,
          true,
          Metadata.empty
        )(NamedExpression.newExprId, Seq.empty[String])

//        inputGeom.references
        val geometryAPI = GeometryAPI(geometryAPIName)
        val geom = geometryAPI.geometry(input1, inputGeom.dataType)
        geom.getSpatialReference
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(1).map(_.asInstanceOf[Expression])
        val res = ST_Length(asArray(0), geometryAPIName)
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

              geometryAPIName match {
                  case "ESRI" => s"""
                                    |$inCode
                                    |${ev.value} = $geomInRef.getEsriSpatialReference().getID();
                                    |""".stripMargin
                  case "JTS"  => s"""
                                   |try {
                                   |$inCode
                                   |${ev.value} = $geomInRef.getSRID();
                                   |} catch (Exception e) {
                                   | throw e;
                                   |}
                                   |""".stripMargin

              }
          }
        )

    override protected def withNewChildInternal(newChild: Expression): Expression = copy(inputGeom = newChild)

}

object ST_SRID {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_SRID].getCanonicalName,
          db.orNull,
          "st_srid",
          """
            |    _FUNC_(expr1) - Returns the Spatial Reference Identifier for a given geometry.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a);
            |        27700
            |  """.stripMargin,
          "",
          "misc_funcs",
          "1.0",
          "",
          "built-in"
        )
}
