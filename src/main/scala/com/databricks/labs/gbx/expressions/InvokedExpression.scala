package com.databricks.labs.gbx.expressions

import org.apache.spark.sql.catalyst.expressions.{ImplicitCastInputTypes, Literal, RuntimeReplaceable}
import org.apache.spark.sql.types.{BinaryType, DataType, ObjectType, StringType}

trait InvokedExpression extends RuntimeReplaceable with ImplicitCastInputTypes {

    override def inputTypes: Seq[DataType] = children.map(_.dataType)

    def invoke(companion: Object, methodName: String = "eval"): PrettyInvoke = {
        val moduleLiteral = Literal.create(
          companion,
          ObjectType(companion.getClass)
        )

        // call the eval method on the companion object
        // this isn't a classic static method call, but a
        // call to a method on a singleton object
        new PrettyInvoke(
          exprName = companion.asInstanceOf[WithExpressionInfo].name,
          targetObject = moduleLiteral,
          functionName = methodName,
          dataType = dataType,
          arguments = children,
          methodInputTypes = inputTypes,
          propagateNull = true,
          returnNullable = true,
          isDeterministic = true
        )
    }

    def rstInvoke(companion: Object, rdt: DataType): PrettyInvoke = {
        rdt match {
            case StringType => invoke(companion, "evalPath")
            case BinaryType => invoke(companion, "evalBinary")
        }
    }

}
