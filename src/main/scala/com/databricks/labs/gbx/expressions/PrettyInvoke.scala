package com.databricks.labs.gbx.expressions

import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types.DataType

class PrettyInvoke(
    exprName: String,
    targetObject: Expression,
    functionName: String,
    dataType: DataType,
    arguments: Seq[Expression] = Nil,
    methodInputTypes: Seq[DataType] = Nil,
    propagateNull: Boolean = true,
    returnNullable: Boolean = true,
    isDeterministic: Boolean = true
) extends Invoke(
      targetObject,
      functionName,
      dataType,
      arguments,
      methodInputTypes,
      propagateNull,
      returnNullable,
      isDeterministic
    ) {

    override def toString(): String = {
        val args = arguments.map {
            case literal: Literal if literal.value.toString.length > 20 => s"literal(...)"
            case arg => arg.toString()
        }.mkString(", ")
        val targetClass = targetObject.toString().split("\\$").headOption.getOrElse("Unknown")
        s"$exprName($args)@$targetClass"
    }

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): PrettyInvoke =
        new PrettyInvoke(
          exprName = exprName,
          targetObject = nc.head,
          functionName = functionName,
          dataType = dataType,
          arguments = nc.tail,
          methodInputTypes = methodInputTypes,
          propagateNull = propagateNull,
          returnNullable = returnNullable,
          isDeterministic = isDeterministic
        )

}
