package com.databricks.labs.mosaic.functions

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ExpressionInfo}
import org.apache.spark.sql.catalyst.FunctionIdentifier

import scala.reflect.ClassTag

case class MosaicFunctionRegistry(
    registry: FunctionRegistry,
    database: Option[String] = None,
    indexSystem: IndexSystem,
    geometryAPI: GeometryAPI
) {

    def registerFunction[T <: Expression: ClassTag](
        name: String
    ): Unit = {
        registry.registerFunction(
          FunctionIdentifier(name, database),
          expressionInfoImpl[T](name, database),
          (exprs: Seq[Expression]) => instance[T](exprs)
        )
    }

    private def expressionInfoImpl[T: ClassTag](
        name: String,
        db: Option[String] = None
    ): ExpressionInfo = {
        val clazz = scala.reflect.classTag[T].runtimeClass
        val df = clazz.getAnnotation(classOf[ExpressionDescription])
        if (df != null) {
            if (df.extended().isEmpty) {
                val group = df.group() match {
                    case "geometry_funcs" => "misc_funcs"
                    case _                => df.group()
                }
                new ExpressionInfo(
                  clazz.getCanonicalName, // className
                  db.orNull, // db
                  name, // name
                  df.usage(), // usage
                  df.arguments(), // arguments
                  df.examples(), // examples
                  df.note(), // note
                  group, // group
                  df.since(), // since
                  df.deprecated(), // deprecated
                  df.source() // source
                )
            } else {
                // This exists for the backward compatibility with old `ExpressionDescription`s defining
                // the extended description in `extended()`.
                new ExpressionInfo(clazz.getCanonicalName, db.orNull, name, df.usage(), df.extended())
            }
        } else {
            new ExpressionInfo(clazz.getCanonicalName, name)
        }
    }

    private def instance[T: ClassTag](
        exprArgs: Seq[Expression]
    ): Expression = {
        val clazz = scala.reflect.classTag[T].runtimeClass
        val args = exprArgs ++ Seq(indexSystem.name, geometryAPI.name)
        val clazzes = args.map(_.getClass).map {
            case c if c.getCanonicalName.contains("org.apache.spark.sql.catalyst.expressions")    => classOf[Expression]
            case c                                                                                => c
        }
        val constructor = clazz.getConstructor(clazzes: _*)
        constructor.newInstance(args: _*).asInstanceOf[Expression]
    }

}
