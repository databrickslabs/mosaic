package com.databricks.labs.mosaic.expressions.base

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType

import scala.reflect.ClassTag

abstract class GeometryAttributeExpression[T <: Expression: ClassTag](inputGeom: Expression, outputType: DataType)
    extends UnaryExpression
      with NullIntolerant
      with Serializable {

    protected val geometryAPI: GeometryAPI = MosaicContext.geometryAPI()

    /**
      * ST_Area expression returns area covered by the
      * [[org.locationtech.jts.geom.Geometry]] instance extracted from inputGeom
      * expression.
      */

    override def child: Expression = inputGeom

    /** Output Data Type */
    override def dataType: DataType = outputType

    def geomTransform(geometry: MosaicGeometry): Any

    def geomTransformCodeGen(ctx: CodegenContext, geomRef: String): (String, String)

    override def nullSafeEval(input1: Any): Any = {
        val geom = geometryAPI.geometry(input1, inputGeom.dataType)
        geomTransform(geom)
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = GenericExpressionFactory.makeCopyImpl[T](this, newArgs, 1)

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
        nullSafeCodeGen(
          ctx,
          ev,
          eval => {
              val (inCode, geomInRef) = ConvertToCodeGen.readGeometryCode(ctx, eval, inputGeom.dataType, geometryAPI)
              val (resultCode, resultGeomRef) = geomTransformCodeGen(ctx, geomInRef)
              geometryAPI.codeGenTryWrap(s"""
                                            |$inCode
                                            |$resultCode
                                            |${ev.value} = $resultGeomRef;
                                            |""".stripMargin)
          }
        )

    override protected def withNewChildInternal(newChild: Expression): Expression = makeCopy(Array(newChild))

}
