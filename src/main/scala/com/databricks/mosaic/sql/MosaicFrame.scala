package com.databricks.mosaic.sql

import com.databricks.mosaic.core.geometry.MosaicGeometry
import com.databricks.mosaic.core.geometry.api.GeometryAPI
import org.apache.spark.sql._
import com.databricks.mosaic.core.types.model.GeometryTypeEnum
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.sql.join.PointInPolygonJoin


class MosaicFrame private (var df: DataFrame, geometryColumnName: String, geometryType: Option[GeometryTypeEnum.Value] = None) extends MosaicAnalyzer {

  val mosaicContext: MosaicContext = MosaicContext.context
  import ss.implicits._
  import mosaicContext.functions._


  geometryColumn = df.col(geometryColumnName)
  geometryColumnType = geometryColumn.expr.dataType

  def getGeometryType: GeometryTypeEnum.Value = geometryType match {
    case Some(g) => g
    case None =>
      val geometryTypeString: String = df.limit(1).select(st_geometrytype(geometryColumn)).as[String].collect.head
      GeometryTypeEnum.fromString(geometryTypeString)
  }
  analyzerDataFrame = df

//  val chipColumn: StructField = StructField("chip_wkb", BinaryType, nullable = true, new MetadataBuilder().putString("geo_encoding", "wkb").build())
//  val chipFlagColumn: StructField = StructField("is_core", BooleanType, nullable = false)
//  val indexColumn: StructField = StructField("mosaic_index", LongType, nullable = false)

  private val chipColumnName = "chip_geometry"
  def chipColumn: Column = df.col(chipColumnName)
  private val chipFlagColumnName = "is_core"
  def chipFlagColumn: Column = df.col(chipFlagColumnName)
  private val indexColumnName = "h3"
  def indexColumn: Column = df.col(indexColumnName)

  private var indexed: Boolean = false
  private var indexResolution: Option[Int] = None

  def setIndexResolution(resolution: Int): Unit = indexResolution = Some(resolution)
  def getIndexResolution: Int = indexResolution match {
    case Some(i) => i
    case _ => throw MosaicSQLExceptions.NoIndexResolutionSet
  }


  def toDataset: Dataset[(MosaicGeometry, Long, Any, Boolean)] = {

    df.select(
      geometryColumn,
      indexColumn,
      chipColumn,
      chipFlagColumn
    ).map { row =>
      (
        mosaicContext.getGeometryAPI.geometry(row.get(0), geometryColumnType),
        row.getLong(1),
        row.get(2),
        row.getBoolean(3)
      )
    }
  }

  def applyIndex(): Unit = {
    indexResolution match {
      case Some(i) => applyIndex(i)
      case None => throw MosaicSQLExceptions.NoIndexResolutionSet
    }
  }

  private def applyIndex(resolution: Int): Unit = {
    getGeometryType match {
      case GeometryTypeEnum.POLYGON =>
        df = df.select($"*", mosaic_explode(geometryColumn, resolution).as(Seq(chipFlagColumnName, indexColumnName, chipColumnName)))
      case GeometryTypeEnum.POINT =>
        df = df.select($"*", point_index(geometryColumn, resolution).as(indexColumnName))
      case _ =>
    }
    indexed = true
  }

  def join(other: MosaicFrame): DataFrame = {
    if (!indexed) throw MosaicSQLExceptions.MosaicFrameNotIndexed
    val joinedDf = (getGeometryType, other.getGeometryType) match {
      case (GeometryTypeEnum.POINT, GeometryTypeEnum.POLYGON) => Some(PointInPolygonJoin.join(this, other))
      case (GeometryTypeEnum.POLYGON, GeometryTypeEnum.POINT) => Some(PointInPolygonJoin.join(other, this))
      case (GeometryTypeEnum.POLYGON, GeometryTypeEnum.POLYGON) => None // polygon intersection join
      case (GeometryTypeEnum.POINT, GeometryTypeEnum.POINT) => None // range join
      case _ => None
    }
    if (joinedDf.isEmpty) throw MosaicSQLExceptions.SpatialJoinTypeNotSupported(getGeometryType, other.getGeometryType)
    joinedDf.get
  }

  def prettified: DataFrame = Prettifier.prettified(df)

  def show(): Unit = df.show()

}

object MosaicFrame {

//  def prettify(df: DataFrame): DataFrame = MosaicFrame(df).prettified

  def apply(
             df: DataFrame,
             geometryColumnName: String
           ): MosaicFrame = new MosaicFrame(df, geometryColumnName: String, None)

  def apply(
               df: DataFrame,
               geometryColumnName: String,
               geometryType: GeometryTypeEnum.Value
           ): MosaicFrame = new MosaicFrame(df, geometryColumnName, Some(geometryType))

}
