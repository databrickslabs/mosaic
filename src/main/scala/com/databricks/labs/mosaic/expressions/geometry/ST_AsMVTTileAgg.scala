package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.expressions.geometry.base.AsTileExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import com.databricks.labs.mosaic.utils.{PathUtils, SysUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.trees.TernaryLike
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.gdal.ogr._

import java.nio.file.{Files, Paths}
import scala.collection.mutable

case class ST_AsMVTTileAgg(
    geometryExpr: Expression,
    attributesExpr: Expression,
    zxyIDExpr: Expression,
    expressionConfig: MosaicExpressionConfig,
    mutableAggBufferOffset: Int,
    inputAggBufferOffset: Int
) extends TypedImperativeAggregate[mutable.ArrayBuffer[Any]]
      with TernaryLike[Expression]
      with AsTileExpression {

    val geometryAPI: GeometryAPI = GeometryAPI.apply(expressionConfig.getGeometryAPI)
    override lazy val deterministic: Boolean = true
    override val first: Expression = geometryExpr
    override val second: Expression = attributesExpr
    override val third: Expression = zxyIDExpr
    override val nullable: Boolean = false
    override val dataType: DataType = BinaryType

    override def prettyName: String = "st_asmvttile_agg"

    // The tiling scheme for the MVT: https://gdal.org/drivers/vector/mvt.html
    private val tilingScheme3857 = "EPSG:3857,-20037508.343,20037508.343,40075016.686"
    private val tilingScheme4326 = "EPSG:4326,-180,180,360"

    private lazy val tupleType =
        StructType(
          StructField("geom", geometryExpr.dataType, nullable = false) ::
              StructField("attrs", attributesExpr.dataType, nullable = false) ::
              StructField("zxyID", zxyIDExpr.dataType, nullable = false) ::
              Nil
        )
    private lazy val projection = UnsafeProjection.create(Array[DataType](ArrayType(elementType = tupleType, containsNull = false)))
    private lazy val row = new UnsafeRow(2)

    override def createAggregationBuffer(): mutable.ArrayBuffer[Any] = mutable.ArrayBuffer.empty

    def update(buffer: mutable.ArrayBuffer[Any], input: InternalRow): mutable.ArrayBuffer[Any] = {
        val geom = geometryExpr.eval(input)
        val attrs = attributesExpr.eval(input)
        val zxyID = zxyIDExpr.eval(input)
        val value = InternalRow.fromSeq(Seq(geom, attrs, zxyID))
        buffer += InternalRow.copyValue(value)
        buffer
    }

    def merge(buffer: mutable.ArrayBuffer[Any], input: mutable.ArrayBuffer[Any]): mutable.ArrayBuffer[Any] = {
        buffer ++= input
    }

    override def eval(buffer: mutable.ArrayBuffer[Any]): Any = {
        ogr.RegisterAll()
        // We assume all zxyIDs are the same for all the rows in the buffer
        val zxyID = buffer.head.asInstanceOf[InternalRow].get(2, zxyIDExpr.dataType).toString
        val zoom = zxyID.split("/")(0).toInt
        val driver = ogr.GetDriverByName("MVT")
        val tmpName = PathUtils.createTmpFilePath("mvt")

        val srs = getSRS(buffer.head, geometryExpr, geometryAPI)
        val tilingScheme = srs.GetAttrValue("PROJCS", 0) match {
            case "WGS 84 / Pseudo-Mercator" => tilingScheme3857
            case "WGS 84"                   => tilingScheme4326
            case _                          => throw new Error(s"Unsupported SRS: ${srs.GetAttrValue("PROJCS", 0)}")
        }

        val createOptions = new java.util.Vector[String]()
        createOptions.add("NAME=mvttile")
        createOptions.add("TYPE=baselayer")
        createOptions.add(s"MINZOOM=$zoom")
        createOptions.add(s"MAXZOOM=$zoom")
        createOptions.add(s"TILING_SCHEME=$tilingScheme")

        val ds: DataSource = driver.CreateDataSource(tmpName, createOptions)

        val layer = createLayer(ds, srs, attributesExpr.dataType.asInstanceOf[StructType])

        insertRows(buffer, layer, geometryExpr, geometryAPI, attributesExpr)

        ds.FlushCache()
        ds.delete()

        val tiles = SysUtils
            .runCommand(s"ls $tmpName")
            ._1
            .split("\n")
            .filterNot(_.endsWith(".json"))
            .flatMap(z =>
                SysUtils
                    .runCommand(s"ls $tmpName/$z")
                    ._1
                    .split("\n")
                    .flatMap(x =>
                        SysUtils
                            .runCommand(s"ls $tmpName/$z/$x")
                            ._1
                            .split("\n")
                            .map(y => s"$tmpName/$z/$x/$y")
                    )
            )

        Files.readAllBytes(Paths.get(tiles.head))

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

    override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): ST_AsMVTTileAgg =
        copy(geometryExpr = newFirst, attributesExpr = newSecond, zxyIDExpr = newThird)

}

object ST_AsMVTTileAgg {

    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_AsMVTTileAgg].getCanonicalName,
          db.orNull,
          "st_asmvttile_agg",
          """
            |    _FUNC_(geom, attrs) - Returns a Mapbox Vector Tile (MVT) as a binary.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT st_asmvttile_agg(geom, attrs) FROM table;
            |       0x1A2B3C4D5E6F
            |       0x1A2B3C4D5E6F
            """.stripMargin,
          "",
          "agg_funcs",
          "1.0",
          "",
          "built-in"
        )

}
