package com.databricks.labs.mosaic.expressions.geometry

import org.apache.spark.sql.catalyst.expressions.UnaryExpression
import org.apache.spark.sql.catalyst.expressions.NullIntolerant
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.DataType
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback

case class ST_UnaryUnion(inputGeom: Expression, geometryAPIName: String) extends UnaryExpression with NullIntolerant with CodegenFallback {

    override def child: Expression = inputGeom

    override def dataType: DataType = inputGeom.dataType

    override protected def nullSafeEval(input: Any): Any = {
        val geometryAPI = GeometryAPI(geometryAPIName)
        val geometry = geometryAPI.geometry(input, inputGeom.dataType)
        val unaryUnion = geometry.unaryUnion
        geometryAPI.serialize(unaryUnion, inputGeom.dataType)
    }

    override protected def withNewChildInternal(newChild: Expression): Expression = copy(inputGeom = newChild)

}
