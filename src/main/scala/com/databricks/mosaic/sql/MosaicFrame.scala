package com.databricks.mosaic.sql

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.adapters.MosaicDataset
import org.apache.spark.internal.Logging
import com.databricks.mosaic.core.types.model.GeometryTypeEnum
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.sql.join.PointInPolygonJoin
import org.apache.spark.sql.types.{DataType, MetadataBuilder}

class MosaicFrame(
    df: DataFrame,
    geometryColumnName: String,
    geometryType: Option[GeometryTypeEnum.Value] = None,
    indexed: Boolean = false,
    indexResolution: Option[Int] = None
) extends MosaicDataset(df)
      with Logging {

    val mosaicContext: MosaicContext = MosaicContext.context
    import mosaicContext.functions._
    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    def analyzer: MosaicAnalyzer = new MosaicAnalyzer(this)

//    val geometryMetadata = new MetadataBuilder().putString("k", "v").build()
    val geometryColumn: Column = this.col(geometryColumnName)
//        .as(geometryColumnName, geometryMetadata)
//    this.schema(0).metadata
    val geometryColumnType: DataType = geometryColumn.expr.dataType
    private var chipColumnName = "chip_geometry"
    private var chipFlagColumnName = "is_core"
    private var indexColumnName = "h3"

    def chipColumn: Column = this.col(chipColumnName)
    def chipFlagColumn: Column = this.col(chipFlagColumnName)
    def indexColumn: Column = this.col(indexColumnName)

    def isIndexed: Boolean = indexed

    def applyIndex(): MosaicFrame = {
        indexResolution match {
            case Some(i) => applyIndex(i)
            case None    => throw MosaicSQLExceptions.NoIndexResolutionSet
        }
    }

    private def applyIndex(resolution: Int): MosaicFrame = {
        val indexedDf = getGeometryType match {
            case GeometryTypeEnum.POLYGON      =>
                this.select($"*", mosaic_explode(geometryColumn, resolution).as(Seq(chipFlagColumnName, indexColumnName, chipColumnName)))
            case GeometryTypeEnum.POINT        => this.select($"*", point_index(geometryColumn, resolution).as(indexColumnName))
            case GeometryTypeEnum.MULTIPOLYGON =>
                val flattenGeometryColumnName = s"${geometryColumnName}_flat"
                val flattenedDf = this.select(col("*"), flatten_polygons(geometryColumn).as(flattenGeometryColumnName))
                flattenedDf.select(
                  col("*"),
                  mosaic_explode(flattenedDf.col(flattenGeometryColumnName), resolution)
                      .as(Seq(chipFlagColumnName, indexColumnName, chipColumnName))
                )
            case _                             => this
        }
        new MosaicFrame(indexedDf, this.geometryColumnName, this.geometryType, true, this.indexResolution)
    }

    def getGeometryType: GeometryTypeEnum.Value =
        geometryType match {
            case Some(g) => g
            case None    =>
                val geometryTypeString: String = df.limit(1).select(st_geometrytype(geometryColumn)).as[String].collect.head
                GeometryTypeEnum.fromString(geometryTypeString)
        }

    def join(other: MosaicFrame): MosaicFrame = {
        if (!(this.isIndexed & other.isIndexed)) throw MosaicSQLExceptions.MosaicFrameNotIndexed
        val joinedDf = (getGeometryType, other.getGeometryType) match {
            case (GeometryTypeEnum.POINT, GeometryTypeEnum.POLYGON)      => Some(PointInPolygonJoin.join(this, other))
            case (GeometryTypeEnum.POLYGON, GeometryTypeEnum.POINT)      => Some(PointInPolygonJoin.join(other, this))
            case (GeometryTypeEnum.POINT, GeometryTypeEnum.MULTIPOLYGON) => Some(PointInPolygonJoin.join(this, other))
            case (GeometryTypeEnum.MULTIPOLYGON, GeometryTypeEnum.POINT) => Some(PointInPolygonJoin.join(other, this))
            case (GeometryTypeEnum.POLYGON, GeometryTypeEnum.POLYGON)    => None // polygon intersection join
            case (GeometryTypeEnum.POINT, GeometryTypeEnum.POINT)        => None // range join
            case _                                                       => None
        }
        joinedDf.getOrElse(throw MosaicSQLExceptions.SpatialJoinTypeNotSupported(getGeometryType, other.getGeometryType))
    }

    //    def toDataset: Dataset[(MosaicGeometry, Long, Any, Boolean)] = {
    //
    //        df.select(
    //            geometryColumn,
    //            indexColumn,
    //            chipColumn,
    //            chipFlagColumn
    //        ).map { row =>
    //            (
    //                mosaicContext.getGeometryAPI.geometry(row.get(0), geometryColumnType),
    //                row.getLong(1),
    //                row.get(2),
    //                row.getBoolean(3)
    //            )
    //        }
    //    }

    def prettified: DataFrame = Prettifier.prettified(df)

//    def copy: MosaicFrame = new MosaicFrame(this.df, this.geometryColumnName, this.geometryType, this.indexed, this.indexResolution)

    def setIndexResolution(resolution: Int): MosaicFrame =
        new MosaicFrame(this.df, this.geometryColumnName, this.geometryType, this.indexed, Some(resolution))

    def getGeometryColumnName: String = this.geometryColumnName

    def getOptimalResolution(sampleFraction: Double): Int = {
        analyzer.getOptimalResolution(sampleFraction)
    }

    def getOptimalResolution(sampleRows: Int): Int = {
        analyzer.getOptimalResolution(sampleRows)
    }

    def getOptimalResolution: Int = {
        analyzer.getOptimalResolution(analyzer.defaultSampleFraction)
    }

//    def getResolutionMetrics(sampleFraction: Double = analyzer.analyzerSampleFraction): Int = {
//        sampleFraction match {
//            case d if (d > 0d & d <= 1d) => analyzer.getOptimalResolution(d)
//            case _ => None
//        }

    def getIndexResolution: Int =
        indexResolution match {
            case Some(i) => i
            case _       => throw MosaicSQLExceptions.NoIndexResolutionSet
        }

    def withPrefix(prefix: String): MosaicFrame = {
        def prepend(str: String) = s"${prefix}_$str"
        val prefixed = select(columns.map(c => col(c).alias(prepend(c))): _*)
        val newMosaicFrame = new MosaicFrame(
          prefixed,
          prepend(this.geometryColumnName),
          geometryType,
          indexed,
          indexResolution
        )
        newMosaicFrame.chipColumnName = prepend(newMosaicFrame.chipColumnName)
        newMosaicFrame.indexColumnName = prepend(newMosaicFrame.indexColumnName)
        newMosaicFrame.chipFlagColumnName = prepend(newMosaicFrame.chipFlagColumnName)
        newMosaicFrame
    }

}

object MosaicFrame {

    //  def prettify(df: DataFrame): DataFrame = MosaicFrame(df).prettified

    def apply(
        df: DataFrame,
        geometryColumnName: String
    ): MosaicFrame = new MosaicFrame(df, geometryColumnName: String, None, false, None)

    def apply(
        df: DataFrame,
        geometryColumnName: String,
        geometryType: GeometryTypeEnum.Value
    ): MosaicFrame = new MosaicFrame(df, geometryColumnName, Some(geometryType), false, None)

}
