package com.dblabs.spatial.expressions

import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.catalyst.expressions.{ImplicitCastInputTypes, Literal, RuntimeReplaceable}
import org.apache.spark.sql.types.{DataType, ObjectType}

trait InvokedExpression extends RuntimeReplaceable with ImplicitCastInputTypes {

    override def inputTypes: Seq[DataType] = children.map(_.dataType)

    def invoke(companion: Object): Invoke = {
        val moduleLiteral = Literal.create(
          companion,
          ObjectType(companion.getClass)
        )

        // call the eval method on the companion object
        // this isn't a classic static method call, but a
        // call to a method on a singleton object
        Invoke(
          targetObject = moduleLiteral,
          functionName = "eval",
          dataType = dataType,
          arguments = children,
          methodInputTypes = inputTypes,
          propagateNull = true,
          returnNullable = true,
          isDeterministic = true
        )
    }

}
