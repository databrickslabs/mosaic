// Databricks notebook source
package com.databricks.mosaic.patch

import com.databricks.mosaic.expressions.geometry.FlattenPolygons
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, ExpressionDescription, UnaryExpression}
import org.apache.spark.sql.types._
import scala.collection.TraversableOnce

/**
 * A hotfix for replacing FlattenPolygons expression that at runtime 
 * becomes abstract due to CollectioGenerator inheritace clashes.
 */
case class FlattenPolygonsPatch(pair: Expression, geometryAPIName: String)
  extends UnaryExpression with CollectionGenerator with CodegenFallback {

  override val inline: Boolean = false
  override def collectionType: DataType = child.dataType
  override def child: Expression = pair
  override def position: Boolean = false
  override def checkInputDataTypes(): TypeCheckResult = FlattenPolygons.checkInputDataTypesImpl(child)
  override def elementSchema: StructType = FlattenPolygons.elementSchemaImpl(child)
  override def eval(input: InternalRow): TraversableOnce[InternalRow] = FlattenPolygons.evalImpl(input, child, geometryAPIName)
  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val asArray = newArgs.take(1).map(_.asInstanceOf[Expression])
    val res = FlattenPolygonsPatch(asArray(0), geometryAPIName)
    res.copyTagsFrom(this)
    res
  }
    
}

// COMMAND ----------

package com.databricks.mosaic.patch

import com.databricks.mosaic.expressions.index.MosaicExplode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, ExpressionDescription, UnaryExpression}
import org.apache.spark.sql.types._
import scala.collection.TraversableOnce

/**
 * A hotfix for replacing MosaicExplode expression that at runtime 
 * becomes abstract due to CollectioGenerator inheritace clashes.
 */
case class MosaicExplodePatch(pair: Expression, indexSystemName: String, geometryAPIName: String)
  extends UnaryExpression with CollectionGenerator with Serializable with CodegenFallback {

  override val inline: Boolean = false
  override def collectionType: DataType = child.dataType
  override def child: Expression = pair
  override def position: Boolean = false
  override def checkInputDataTypes(): TypeCheckResult = MosaicExplode.checkInputDataTypesImpl(child)
  override def elementSchema: StructType = MosaicExplode.elementSchemaImpl(child)
  override def eval(input: InternalRow): TraversableOnce[InternalRow] = MosaicExplode.evalImpl(input, child, indexSystemName, geometryAPIName)
  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val arg1 = newArgs.head.asInstanceOf[Expression]
    val res = MosaicExplodePatch(arg1, indexSystemName, geometryAPIName)
    res.copyTagsFrom(this)
    res
  }
}

// COMMAND ----------

package com.databricks.mosaic.patch

import com.databricks.mosaic.core.index.IndexSystem
import com.databricks.mosaic.core.geometry.api.GeometryAPI

case class MosaicPatch(indexSystem: IndexSystem, geometryAPI: GeometryAPI) {
  import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
  import com.databricks.mosaic.patch._
  import org.apache.spark.sql.{Column, SparkSession}
  import org.apache.spark.sql.catalyst.FunctionIdentifier
  import org.apache.spark.sql.catalyst.expressions.Expression
  import org.apache.spark.sql.functions.{lit, struct}
  
  def register(spark: SparkSession, database: Option[String] = None): Unit = {
    val registry = spark.sessionState.functionRegistry

    registry.registerFunction(FunctionIdentifier("flatten_polygons", database), (exprs: Seq[Expression]) => FlattenPolygonsPatch(exprs(0), geometryAPI.name))
    registry.registerFunction(
      FunctionIdentifier("mosaic_explode", database),
      (exprs: Seq[Expression]) => MosaicExplodePatch(struct(ColumnAdapter(exprs(0)), ColumnAdapter(exprs(1))).expr, indexSystem.name, geometryAPI.name)
    )
  } 

  object functions {
    def flatten_polygons(geom: Column): Column = ColumnAdapter(FlattenPolygonsPatch(geom.expr, geometryAPI.name))
    def mosaic_explode(geom: Column, resolution: Column): Column = ColumnAdapter(MosaicExplodePatch(struct(geom, resolution).expr, indexSystem.name, geometryAPI.name))
    def mosaic_explode(geom: Column, resolution: Int): Column = ColumnAdapter(MosaicExplodePatch(struct(geom, lit(resolution)).expr, indexSystem.name, geometryAPI.name))
  }
}
