package com.dblabs.spatial.expressions

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ExpressionInfo}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder

import scala.collection.immutable
import scala.reflect.runtime.universe
import scala.reflect.ClassTag
import scala.util.Try

case class RegistryDelegate(registry: FunctionRegistry, database: Option[String] = None) {

    private def getAnnotations[T: universe.TypeTag]: immutable.Seq[universe.Annotation] = {
        universe.typeOf[T].members.foldLeft(List.empty[universe.Annotation]) { (acc, member) => acc ++ member.annotations }
    }

    private def getCompanion[T: universe.TypeTag]: Any = {
        universe.runtimeMirror(getClass.getClassLoader).reflectModule(universe.typeOf[T].typeSymbol.companion.asModule).instance
    }

    def registerExpression[T <: Expression: universe.TypeTag: ClassTag](expressionConfig: ExpressionConfig): Unit =
        registerExpression[T](None, None, expressionConfig)

    def registerExpression[T <: Expression: universe.TypeTag: ClassTag](alias: String, expressionConfig: ExpressionConfig): Unit =
        registerExpression[T](alias = Some(alias), None, expressionConfig)

    def registerExpression[T <: Expression: universe.TypeTag: ClassTag](
        builder: FunctionBuilder,
        expressionConfig: ExpressionConfig
    ): Unit = registerExpression[T](None, builder = Some(builder), expressionConfig)

    def registerExpression[T <: Expression: universe.TypeTag: ClassTag](
        alias: String,
        builder: FunctionBuilder,
        expressionConfig: ExpressionConfig
    ): Unit = registerExpression[T](alias = Some(alias), builder = Some(builder), expressionConfig)

    private def registerExpression[T <: Expression: universe.TypeTag: ClassTag](
        alias: Option[String],
        builder: Option[FunctionBuilder],
        expressionConfig: ExpressionConfig
    ): Unit = {
        Try {
            val companion = getCompanion[T].asInstanceOf[WithExpressionInfo]
            val annotations = getAnnotations[T]
            val expressionInfoVal = annotations
                .find(_.tree.tpe =:= universe.typeOf[ExpressionDescription])
                .getOrElse(
                  companion.getExpressionInfo[T]()
                )
                .asInstanceOf[ExpressionInfo]
            val builderVal = builder.getOrElse(companion.builder(expressionConfig))
            val nameVal = alias.getOrElse(companion.name)

            registry.registerFunction(
              FunctionIdentifier(nameVal, database),
              expressionInfoVal,
              builderVal
            )
        } match {
            case scala.util.Success(_) => ()
            case scala.util.Failure(e) => throw new Exception(s"Failed to register expression: ${e.getMessage}")
        }
    }

}
