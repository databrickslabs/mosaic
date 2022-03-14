package com.databricks.labs.mosaic.expressions.index

import scala.collection.TraversableOnce

import com.databricks.labs.mosaic.core.Mosaic
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystemID
import com.databricks.labs.mosaic.core.types._
import org.locationtech.jts.geom.Geometry

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, ExpressionInfo, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

case class MosaicExplode(pair: Expression, indexSystemName: String, geometryAPIName: String)
    extends UnaryExpression
      with CollectionGenerator
      with Serializable
      with CodegenFallback {

    override val inline: Boolean = false

    override def checkInputDataTypes(): TypeCheckResult = MosaicExplode.checkInputDataTypesImpl(child)

    override def elementSchema: StructType = MosaicExplode.elementSchemaImpl(child)

    override def child: Expression = pair

    override def eval(input: InternalRow): TraversableOnce[InternalRow] =
        MosaicExplode.evalImpl(input, child, indexSystemName, geometryAPIName)

    override def makeCopy(newArgs: Array[AnyRef]): Expression = MosaicExplode.makeCopyImpl(newArgs, this, indexSystemName, geometryAPIName)

    override def collectionType: DataType = child.dataType

    override def position: Boolean = false

    override protected def withNewChildInternal(newChild: Expression): Expression = copy(pair = newChild)

}

object MosaicExplode {

    /**
      * Type-wise differences in evaluation are only present on the input data
      * conversion to a [[Geometry]]. The rest of the evaluation is agnostic to
      * the input data type. The evaluation generates a set of core indices that
      * are fully contained by the input [[Geometry]] and a set of border
      * indices that are partially contained by the input [[Geometry]].
      *
      * @param input
      *   Struct containing a geometry and a resolution.
      * @return
      *   A set of serialized
      *   [[com.databricks.mosaic.core.types.model.MosaicChip]]. This set will
      *   be used to generate new rows of data.
      */
    def evalImpl(input: InternalRow, child: Expression, indexSystemName: String, geometryAPIName: String): TraversableOnce[InternalRow] = {
        val inputData = child.eval(input).asInstanceOf[InternalRow]
        val geomType = child.dataType.asInstanceOf[StructType].fields.head.dataType

        val indexSystem = IndexSystemID.getIndexSystem(IndexSystemID(indexSystemName))
        val geometryAPI = GeometryAPI(geometryAPIName)
        val geometry = geometryAPI.geometry(inputData, geomType)

        val resolution = inputData.getInt(1)

        val chips = Mosaic.mosaicFill(geometry, resolution, indexSystem, geometryAPI)

        chips.map(row => InternalRow.fromSeq(Seq(row.serialize)))
    }

    /**
      * [[MosaicExplode]] expression can only be called on supported data types.
      * The supported data types are [[BinaryType]] for WKB encoding,
      * [[StringType]] for WKT encoding, [[HexType]] ([[StringType]] wrapper)
      * for HEX encoding and [[InternalGeometryType]] for primitive types
      * encoding via [[ArrayType]].
      *
      * @return
      *   An instance of [[TypeCheckResult]] indicating success or a failure.
      */
    def checkInputDataTypesImpl(child: Expression): TypeCheckResult = {
        val fields = child.dataType.asInstanceOf[StructType].fields
        val geomType = fields.head
        val resolutionType = fields(1)

        (geomType.dataType, resolutionType.dataType) match {
            case (BinaryType, IntegerType)           => TypeCheckResult.TypeCheckSuccess
            case (StringType, IntegerType)           => TypeCheckResult.TypeCheckSuccess
            case (HexType, IntegerType)              => TypeCheckResult.TypeCheckSuccess
            case (InternalGeometryType, IntegerType) => TypeCheckResult.TypeCheckSuccess
            case _                                   => TypeCheckResult.TypeCheckFailure(
                  s"Input to h3 mosaic explode should be (geometry, resolution) pair. " +
                      s"Geometry type can be WKB, WKT, Hex or Coords. Provided type was: ${child.dataType.catalogString}"
                )
        }
    }

    /**
      * [[MosaicExplode]] is a generator expression. All generator expressions
      * require the element schema to be provided. Chip type is fixed for
      * [[MosaicExplode]], all border chip geometries will be generated as
      * [[BinaryType]] columns encoded as WKBs.
      *
      * @see
      *   [[CollectionGenerator]] for the API of generator expressions.
      *   [[ChipType]] for output type definition.
      * @return
      *   The schema of the child element. Has to be provided as a
      *   [[StructType]].
      */
    def elementSchemaImpl(child: Expression): StructType = {
        val fields = child.dataType.asInstanceOf[StructType].fields
        val geomType = fields.head
        val resolutionType = fields(1)

        (geomType.dataType, resolutionType.dataType) match {
            case (BinaryType, IntegerType)           => StructType(Array(StructField("index", ChipType)))
            case (StringType, IntegerType)           => StructType(Array(StructField("index", ChipType)))
            case (HexType, IntegerType)              => StructType(Array(StructField("index", ChipType)))
            case (InternalGeometryType, IntegerType) => StructType(Array(StructField("index", ChipType)))
            case _                                   => throw new Error(
                  s"Input to h3 mosaic explode should be (geometry, resolution) pair. " +
                      s"Geometry type can be WKB, WKT, Hex or Coords. Provided type was: ${child.dataType.catalogString}"
                )
        }
    }

    def makeCopyImpl(newArgs: Array[AnyRef], instance: Expression, indexSystemName: String, geometryAPIName: String): Expression = {
        val arg1 = newArgs.head.asInstanceOf[Expression]
        val res = MosaicExplode(arg1, indexSystemName, geometryAPIName)
        res.copyTagsFrom(instance)
        res
    }

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[IndexGeometry].getCanonicalName,
          db.orNull,
          "mosaic_explode",
          """
            |    _FUNC_(struct(geometry, resolution)) - Generates the h3 mosaic chips for the input geometry
            |    at a given resolution. Geometry and resolution are provided via struct wrapper to ensure 
            |    UnaryExpression API is respected.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a, b);
            |        {index_id, is_border, chip_geom}
            |        {index_id, is_border, chip_geom}
            |        ...
            |        {index_id, is_border, chip_geom}
            |  """.stripMargin,
          "",
          "generator_funcs",
          "1.0",
          "",
          "built-in"
        )

}
