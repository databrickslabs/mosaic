package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.expressions.geometry.base.AsTileExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import com.databricks.labs.mosaic.utils.PathUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.ogr._

import scala.collection.mutable

case class ST_AsGeojsonTileAgg(
    geometryExpr: Expression,
    attributesExpr: Expression,
    expressionConfig: MosaicExpressionConfig,
    mutableAggBufferOffset: Int,
    inputAggBufferOffset: Int
) extends TypedImperativeAggregate[mutable.ArrayBuffer[Any]]
      with BinaryLike[Expression]
      with AsTileExpression {
    
    val geometryAPI: GeometryAPI = GeometryAPI.apply(expressionConfig.getGeometryAPI)
    override lazy val deterministic: Boolean = true
    override val left: Expression = geometryExpr
    override val right: Expression = attributesExpr
    override val nullable: Boolean = false
    override val dataType: DataType = StringType

    override def prettyName: String = "st_asgeojsontile_agg"

    private lazy val tupleType =
        StructType(
          StructField("geom", geometryExpr.dataType, nullable = false) ::
              StructField("attrs", attributesExpr.dataType, nullable = false) :: Nil
        )
    private lazy val projection = UnsafeProjection.create(Array[DataType](ArrayType(elementType = tupleType, containsNull = false)))
    private lazy val row = new UnsafeRow(2)

    override def createAggregationBuffer(): mutable.ArrayBuffer[Any] = mutable.ArrayBuffer.empty

    def update(buffer: mutable.ArrayBuffer[Any], input: InternalRow): mutable.ArrayBuffer[Any] = {
        val geom = geometryExpr.eval(input)
        val attrs = attributesExpr.eval(input)
        val value = InternalRow.fromSeq(Seq(geom, attrs))
        buffer += InternalRow.copyValue(value)
        buffer
    }

    def merge(buffer: mutable.ArrayBuffer[Any], input: mutable.ArrayBuffer[Any]): mutable.ArrayBuffer[Any] = {
        buffer ++= input
    }

    override def eval(buffer: mutable.ArrayBuffer[Any]): Any = {
        ogr.RegisterAll()
        val driver = ogr.GetDriverByName("GeoJSON")
        val tmpName = PathUtils.createTmpFilePath("geojson")
        val ds: DataSource = driver.CreateDataSource(tmpName)

        val srs = getSRS(buffer.head, geometryExpr, geometryAPI)

        val layer = createLayer(ds, srs, attributesExpr.dataType.asInstanceOf[StructType])
        
        insertRows(buffer, layer, geometryExpr, geometryAPI, attributesExpr)

        ds.FlushCache()
        ds.delete()
        
        val source = scala.io.Source.fromFile(tmpName)
        val result = source.getLines().collect { case x => x }.mkString("\n")
        UTF8String.fromString(result)
    }

    override def serialize(obj: mutable.ArrayBuffer[Any]): Array[Byte] = {
        val array = new GenericArrayData(obj.toArray)
        projection.apply(InternalRow.apply(array)).getBytes
    }

    override def deserialize(bytes: Array[Byte]): mutable.ArrayBuffer[Any] = {
        val buffer = createAggregationBuffer()
        row.pointTo(bytes, bytes.length)
        row.getArray(0).foreach(tupleType, (_, x: Any) => buffer += x)
        buffer
    }

    override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
        copy(inputAggBufferOffset = newInputAggBufferOffset)

    override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
        copy(mutableAggBufferOffset = newMutableAggBufferOffset)

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): ST_AsGeojsonTileAgg =
        copy(geometryExpr = newLeft, attributesExpr = newRight)

}

object ST_AsGeojsonTileAgg {

    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_AsGeojsonTileAgg].getCanonicalName,
          db.orNull,
          "st_asgeojsontile_agg",
          """
            |    _FUNC_(geom, attrs) - Aggregate function that returns a GeoJSON string from a set of geometries and attributes.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a, b) FROM table GROUP BY tile_id;
            |        {"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[1.0,1.0]},"properties":{"name":"a"}},{"type":"Feature","geometry":{"type":"Point","coordinates":[2.0,2.0]},"properties":{"name":"b"}}]}
            |        {"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[3.0,3.0]},"properties":{"name":"c"}},{"type":"Feature","geometry":{"type":"Point","coordinates":[4.0,4.0]},"properties":{"name":"d"}}]}
            |  """.stripMargin,
          "",
          "agg_funcs",
          "1.0",
          "",
          "built-in"
        )

}
