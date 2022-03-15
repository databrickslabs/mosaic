package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.esri.core.geometry.ogc.OGCGeometry
import org.locationtech.jts.geom.{Geometry => JTSGeometry}

import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType

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
              val (outCode, outGeomRef) = ConvertToCodeGen.writeGeometryCode(ctx, convexHull, inputGeom.dataType, geometryAPI)
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

    override protected def withNewChildInternal(newChild: Expression): Expression = copy(inputGeom = newChild)

}

object ST_ConvexHull {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_ConvexHull].getCanonicalName,
          db.orNull,
          "st_convexhull",
          """
            |    _FUNC_(expr1) - Returns the convex hull for a given MultiPoint geometry.
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
