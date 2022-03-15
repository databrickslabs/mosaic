package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.esri.core.geometry._
import com.esri.core.geometry.ogc.OGCGeometry
import org.locationtech.jts.geom.{Geometry => JTSGeometry}

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionInfo, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType

case class ST_Intersection(leftGeom: Expression, rightGeom: Expression, geometryAPIName: String)
    extends BinaryExpression
      with NullIntolerant {

    override def left: Expression = leftGeom

    override def right: Expression = rightGeom

    override def dataType: DataType = leftGeom.dataType

    // noinspection DuplicatedCode
    override def nullSafeEval(input1: Any, input2: Any): Any = {
        val geometryAPI = GeometryAPI(geometryAPIName)
        val geom1 = geometryAPI.geometry(input1, leftGeom.dataType)
        val geom2 = geometryAPI.geometry(input2, rightGeom.dataType)
        val result = geom1.intersection(geom2)
        geometryAPI.serialize(result, leftGeom.dataType)
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
        val res = ST_Intersection(asArray(0), asArray(1), geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
        nullSafeCodeGen(
          ctx,
          ev,
          (leftEval, rightEval) => {
              val geometryAPI = GeometryAPI.apply(geometryAPIName)
              // TODO: code can be simplified if the function is registered and called 2 times
              val (leftInCode, leftGeomInRef) = ConvertToCodeGen.readGeometryCode(ctx, leftEval, leftGeom.dataType, geometryAPI)
              val (rightInCode, rightGeomInRef) = ConvertToCodeGen.readGeometryCode(ctx, rightEval, rightGeom.dataType, geometryAPI)
              val resultGeom = ctx.freshName("result")
              val (outCode, outGeomRef) = ConvertToCodeGen.writeGeometryCode(ctx, resultGeom, leftGeom.dataType, geometryAPI)
              // not merged into the same code block due to JTS IOException throwing
              // ESRI code will always remain simpler
              geometryAPIName match {
                  case "ESRI" =>
                      val operatorIntersectionClass = classOf[OperatorIntersection]
                      val operatorFactoryLocalClass = classOf[OperatorFactoryLocal]
                      val esriOperatorClass = classOf[Operator]
                      val geometryCursorClass = classOf[GeometryCursor]
                      val ogcGeometryClass = classOf[OGCGeometry]
                      val operatorName = ctx.freshName("operator")
                      val cursorName = ctx.freshName("cursor")

                      s"""
                         |$leftInCode
                         |$rightInCode
                         |$operatorIntersectionClass $operatorName = ($operatorIntersectionClass
                         |    $operatorFactoryLocalClass.getInstance().getOperator(
                         |        $esriOperatorClass.Type.Intersection
                         |    )
                         |);
                         |$geometryCursorClass $cursorName = $operatorName.execute(
                         |    $leftGeomInRef.getEsriGeometryCursor, $rightGeomInRef.getEsriGeometryCursor,
                         |    $leftGeomInRef.getEsriSpatialReference, null, -1
                         |);
                         |$ogcGeometryClass $resultGeom = $ogcGeometryClass.createFromEsriCursor(
                         |    $cursorName, $leftGeomInRef.getEsriSpatialReference, true
                         |);
                         |$outCode
                         |${ev.value} = $outGeomRef;
                         |""".stripMargin
                  case "JTS" =>
                      val jtsGeometryClass = classOf[JTSGeometry]
                      s"""
                         |try {
                         |$leftInCode
                         |$rightInCode
                         |$jtsGeometryClass $resultGeom = $leftGeomInRef.intersection($rightGeomInRef);
                         |$outCode
                         |${ev.value} = $outGeomRef;
                         |} catch (Exception e) {
                         | throw e;
                         |}
                         |""".stripMargin

              }
          }
        )

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
        copy(leftGeom = newLeft, rightGeom = newRight)

}

object ST_Intersection {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_Intersection].getCanonicalName,
          db.orNull,
          "st_intersection",
          """
            |    _FUNC_(expr1, expr2) - Returns the intersection of the two geometries.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a, b);
            |        POLYGON(...)
            |  """.stripMargin,
          "",
          "misc_funcs",
          "1.0",
          "",
          "built-in"
        )

}
