package com.databricks.mosaic.expressions.geometry

import com.esri.core.geometry.ogc.OGCGeometry
import org.locationtech.jts.geom.{Geometry => JTSGeometry}

import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType

import com.databricks.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.mosaic.core.geometry.api.GeometryAPI

@ExpressionDescription(
  usage = "_FUNC_(expr1) - Returns the convex hull for a given MultiPoint geometry.",
  examples = """
    Examples:
      > SELECT _FUNC_(a);
       {"POLYGON (( 0 0, 1 0, 1 1, 0 1 ))"}
  """,
  since = "1.0"
)
case class ST_ConvexHull(inputGeom: Expression, geometryAPIName: String) extends UnaryExpression with NullIntolerant {

    override def child: Expression = inputGeom

    override def dataType: DataType = inputGeom.dataType

    override def nullSafeEval(inputRow: Any): Any = {
        val geometryAPI = GeometryAPI(geometryAPIName)
        val geometry = geometryAPI.geometry(inputRow, inputGeom.dataType)
        val convexHull = geometry.convexHull
        geometryAPI.serialize(convexHull, inputGeom.dataType)
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(1).map(_.asInstanceOf[Expression])
        val res = ST_ConvexHull(asArray.head, geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
        nullSafeCodeGen(
          ctx,
          ev,
          leftEval => {
              val geometryAPI = GeometryAPI.apply(geometryAPIName)
              val convexHull = ctx.freshName("convexHull")
              val ogcPolygonClass = classOf[OGCGeometry].getName
              val jtsPolygonClass = classOf[JTSGeometry].getName
              val (inCode, geomInRef) = ConvertToCodeGen.readGeometryCode(ctx, leftEval, inputGeom.dataType, geometryAPI)
              val (outCode, outGeomRef) =  ConvertToCodeGen.writeGeometryCode(ctx, convexHull, inputGeom.dataType, geometryAPI)
              // not merged into the same code block due to JTS IOException throwing
              // OGC code will always remain simpler
              geometryAPIName match {
                  case "OGC" => s"""
                                   |$inCode
                                   |$ogcPolygonClass $convexHull = $geomInRef.convexHull();
                                   |$outCode
                                   |${ev.value} = $outGeomRef;
                                   |""".stripMargin
                  case "JTS" => s"""
                                   |try {
                                   |$inCode
                                   |$jtsPolygonClass $convexHull = $geomInRef.convexHull();
                                   |$outCode
                                   |${ev.value} = $outGeomRef;
                                   |} catch (Exception e) {
                                   | throw e;
                                   |}
                                   |""".stripMargin

              }
          }
        )

}
