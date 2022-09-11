package com.databricks.labs.mosaic.expressions.core

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemID}
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.types.DataType

abstract class MosaicUnaryExpression[T <: Expression](
    childExpr: Expression,
    outputDataType: DataType,
    indexSystemName: Option[String],
    geometryAPIName: Option[String]
) extends UnaryExpression
      with Serializable
      with NullIntolerant {

    private val indexSystemInst: Option[IndexSystem] = indexSystemName.map(name => IndexSystemID.getIndexSystem(IndexSystemID.apply(name)))
    private val geometryAPIInst: Option[GeometryAPI] = geometryAPIName.map(name => GeometryAPI.apply(name))

    def copyImpl(child: Expression): MosaicUnaryExpression[T]

    def indexSystem: IndexSystem = indexSystemInst.get

    def geometryAPI: GeometryAPI = geometryAPIInst.get

    override def child: Expression = childExpr

    override def dataType: DataType = outputDataType

    override protected def withNewChildInternal(newChild: Expression): Expression = makeCopy(Array(newChild))

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(1).map(_.asInstanceOf[Expression])
        val res = copyImpl(asArray(0))
        res.asInstanceOf[expressions.UnaryExpression].copyTagsFrom(this)
        res
    }

}
