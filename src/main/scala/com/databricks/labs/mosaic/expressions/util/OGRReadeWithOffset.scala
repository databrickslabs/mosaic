package com.databricks.labs.mosaic.expressions.util

import com.databricks.labs.mosaic.datasource.OGRFileFormat
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, CollectionGenerator, Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.TraversableOnce

case class OGRReadeWithOffset(pathExpr: Expression, chunkIndexExpr: Expression, config: Map[String, String], schema: StructType)
    extends BinaryExpression
      with CollectionGenerator
      with Serializable
      with CodegenFallback {

    /** Fixed definitions. */
    override val inline: Boolean = false
    val driverName: String = config("driverName")
    val layerNumber: Int = config("layerNumber").toInt
    val layerName: String = config("layerName")
    val chunkSize: Int = config("chunkSize").toInt
    val vsizip: Boolean = config("vsizip").toBoolean

    override def collectionType: DataType = schema

    override def position: Boolean = false

    override def elementSchema: StructType = schema

    override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
        val path = pathExpr.eval(input).asInstanceOf[UTF8String].toString
        val chunkIndex = chunkIndexExpr.eval(input).asInstanceOf[Int]
        OGRFileFormat.enableOGRDrivers()

        val ds = OGRFileFormat.getDataSource(driverName, path, vsizip)
        val layer = OGRFileFormat.getLayer(ds, layerNumber, layerName)

        val start = chunkIndex * chunkSize
        val end = math.min(start + chunkSize, layer.GetFeatureCount()).toInt
        layer.SetNextByIndex(start)
        for (_ <- start until end) yield {
            val feature = layer.GetNextFeature()
            val row = OGRFileFormat.getFeatureFields(feature, schema)
            OGRFileFormat.createRow(row)
        }
    }

    override def left: Expression = pathExpr

    override def right: Expression = chunkIndexExpr

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val pathExpr = newArgs(0).asInstanceOf[Expression]
        val chunkIndexExpr = newArgs(1).asInstanceOf[Expression]
        OGRReadeWithOffset(pathExpr, chunkIndexExpr, config, schema)
    }

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
        makeCopy(Array(newLeft, newRight))

}

object OGRReadeWithOffset {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[OGRReadeWithOffset].getCanonicalName,
          db.orNull,
          "flatten_polygons",
          """
            |    _FUNC_(geometry) - The geometry instance can contain both Polygons and MultiPolygons.
            |    The flattened representation will only contain Polygons.
            |    MultiPolygon rows will be exploded into Polygon rows
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a);
            |        Polygon ((...))
            |        Polygon ((...))
            |        ...
            |        Polygon ((...))
            |  """.stripMargin,
          "",
          "generator_funcs",
          "1.0",
          "",
          "built-in"
        )

}
