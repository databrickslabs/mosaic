package com.databricks.mosaic.sql

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.adapters.MosaicDataset
import org.apache.spark.sql.types._

import com.databricks.mosaic.core.types.model.GeometryTypeEnum
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.sql.MosaicFrame._
import com.databricks.mosaic.sql.join.PointInPolygonJoin

class MosaicFrame(sparkDataFrame: DataFrame) extends MosaicDataset(sparkDataFrame) with Logging {

    val mosaicContext: MosaicContext = MosaicContext.context

    import mosaicContext.functions._

    val spark: SparkSession = sparkDataFrame.sparkSession
    import spark.implicits._

    def setGeometryColumn(geometryColumnName: String): MosaicFrame = {
        val geometryColumn = this.col(geometryColumnName)
        val geometryColumnEncoding = geometryColumnEncodings(geometryColumn.expr.dataType)
        val geometryTypeString: String = inferGeometryType(geometryColumnName).toString
        val geometryType = GeometryTypeEnum.fromString(geometryTypeString)
        if (getFocalGeometryField.isDefined) {

            // An existing column is already configured as the focal geometry.
            // If the column geometry type implies the same indexing scheme we drop the index columns
            // If the column geometry type implies a different indexing scheme, we keep the index columns
            // Mapping from geometry columns to index columns is by geometry column name
            val previousGeometryColumnWithMetadata = this
                .col(getFocalGeometryColumnName)
                .as(
                  getFocalGeometryColumnName,
                  new MetadataBuilder()
                      .withMetadata(getFocalGeometryField.get.metadata)
                      .putBoolean(ColMetaTags.FOCAL_GEOMETRY_FLAG, value = false)
                      .build()
                )

            val geometryMetadata = new MetadataBuilder()
                .withMetadata(getFocalGeometryField.get.metadata)
                .putString(ColMetaTags.ROLE, ColRoles.GEOMETRY)
                .putLong(ColMetaTags.GEOMETRY_ID, geometryCounter.get())
                .putBoolean(ColMetaTags.FOCAL_GEOMETRY_FLAG, value = true)
                .putString(ColMetaTags.GEOMETRY_ENCODING, geometryColumnEncoding)
                .putLong(ColMetaTags.GEOMETRY_TYPE_ID, geometryType.id)
                .putString(ColMetaTags.GEOMETRY_TYPE_DESCRIPTION, geometryType.toString)
                .build()
            val geometryColumnWithMetadata = geometryColumn.as(geometryColumnName, geometryMetadata)
            this
                .withColumn(getFocalGeometryColumnName, previousGeometryColumnWithMetadata)
                .withColumn(geometryColumnName, geometryColumnWithMetadata)
        } else {
            // Initial instance of MosaicFrame
            val geometryMetadata = new MetadataBuilder()
                .putString(ColMetaTags.ROLE, ColRoles.GEOMETRY)
                .putLong(ColMetaTags.GEOMETRY_ID, geometryCounter.get())
                .putBoolean(ColMetaTags.FOCAL_GEOMETRY_FLAG, value = true)
                .putString(ColMetaTags.GEOMETRY_ENCODING, geometryColumnEncoding)
                .putLong(ColMetaTags.GEOMETRY_TYPE_ID, geometryType.id)
                .putString(ColMetaTags.GEOMETRY_TYPE_DESCRIPTION, geometryType.toString)
                .build()
            val geometryColumnWithMetadata = geometryColumn.as(geometryColumnName, geometryMetadata)
            this.withColumn(geometryColumnName, geometryColumnWithMetadata)
        }
    }

    def getPointIndexColumn(indexId: Option[Long] = None): Column = this.col(getPointIndexColumnName(indexId))

    def getPointIndexColumnName(indexId: Option[Long]): String =
        getGeometryAssociatedFieldByRole(ColRoles.INDEX, getGeometryId, indexId) match {
            case Some(f: StructField) => f.name
            case _                    => DefaultColNames.defaultPointIndexColumnName
        }

    def getFillIndexColumn(indexId: Option[Long] = None): Column = this.col(getFillIndexColumnName(indexId))

    def getFillIndexColumnName(indexId: Option[Long]): String =
        getGeometryAssociatedFieldByRole(ColRoles.INDEX, getGeometryId, indexId) match {
            case Some(f: StructField) => f.name
            case _                    => DefaultColNames.defaultFillIndexColumnName
        }

    def getChipColumn(indexId: Option[Long] = None): Column = this.col(getChipColumnName(indexId))

    def getChipColumnName(indexId: Option[Long]): String =
        getGeometryAssociatedFieldByRole(ColRoles.CHIP, getGeometryId, indexId) match {
            case Some(f: StructField) => f.name
            case _                    => DefaultColNames.defaultChipColumnName
        }

    def getGeometryId: Long =
        getFocalGeometryField.getOrElse(throw MosaicSQLExceptions.NoGeometryColumnSet).metadata.getLong(ColMetaTags.GEOMETRY_ID)

    def getFocalGeometryField: Option[StructField] =
        this.schema.fields
            .filter(f => f.metadata.contains("FocalGeometry")) match {
            case fieldsWithFocalLabel: Array[StructField] if !fieldsWithFocalLabel.isEmpty =>
                fieldsWithFocalLabel.filter(f => f.metadata.getBoolean("FocalGeometry")).head match {
                    case f: StructField => Some(f)
                    case _              => None
                }
            case _                                                                         => None
        }

    private def getGeometryAssociatedFieldByRole(role: String, geometryId: Long, indexId: Option[Long]): Option[StructField] = {

        val indexIdCriterion =
            if (indexId.isDefined & ColRoles.AUXILIARIES.contains(role)) {
                Some(ColMetaTags.INDEX_ID -> indexId.get)
            } else None
        val criteria = (Seq(ColMetaTags.PARENT_GEOMETRY_ID -> geometryId, ColMetaTags.ROLE -> role)
            ++ indexIdCriterion)
        this.schema.fields.filter(f => fieldFilter(f, List(ColMetaTags.INDEX_ID))).filter(f => fieldFilter(f, criteria.toMap)) match {
            case f: Array[StructField] if f.nonEmpty => Some(f.maxBy(_.metadata.getLong(ColMetaTags.INDEX_ID)))
            case _                                   => None
        }
    }

    private def fieldFilter(field: StructField, criteria: Map[String, Any]): Boolean =
        criteria.forall({ case (k, v) =>
            if (!field.metadata.contains(k)) false
            else {
                v match {
                    case s: String  => field.metadata.getString(k) == s
                    case i: Int     => field.metadata.getLong(k).toInt == i
                    case l: Long    => field.metadata.getLong(k) == l
                    case b: Boolean => field.metadata.getBoolean(k) == b
                    case _          => false
                }
            }
        })

    private def fieldFilter(field: StructField, tags: List[String]): Boolean = tags.forall(field.metadata.contains)

    def getChipFlagColumn(indexId: Option[Long] = None): Column = this.col(getChipFlagColumnName(indexId))

    def getChipFlagColumnName(indexId: Option[Long]): String =
        getGeometryAssociatedFieldByRole(ColRoles.CHIP_FLAG, getGeometryId, indexId) match {
            case Some(f: StructField) => f.name
            case _                    => DefaultColNames.defaultChipFlagColumnName
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

    //    def prettified: DataFrame = Prettifier.prettified(df)

    def isIndexed: Boolean = getGeometryAssociatedFieldByRole(ColRoles.INDEX, getGeometryId, None).isDefined

    def getGeometryType: GeometryTypeEnum.Value = {
        val geomColField = getFocalGeometryField.getOrElse(throw MosaicSQLExceptions.NoGeometryColumnSet)
        val geomColGeometryType = geomColField.metadata.getLong(ColMetaTags.GEOMETRY_TYPE_ID).toInt
        GeometryTypeEnum.fromId(geomColGeometryType)
    }

    def inferGeometryType(geometryColumnName: String): GeometryTypeEnum.Value = {
        val geomColGeometryType = where(col(geometryColumnName).isNotNull)
            .limit(1)
            .select(st_geometrytype(col(geometryColumnName)))
            .as[String]
            .collect
            .head
        GeometryTypeEnum.fromString(geomColGeometryType)
    }

    def setIndexResolution(resolution: Int): MosaicFrame =
        resolution match {
            case i: Int if mosaicContext.getIndexSystem.minResolution to mosaicContext.getIndexSystem.maxResolution contains i =>
                val geometryColumnMetadata = new MetadataBuilder()
                    .withMetadata(getFocalGeometryField.get.metadata)
                    .putLong(ColMetaTags.INDEX_RESOLUTION, i.toLong)
                    .build()
                val geometryColumnWithMetadata = getGeometryColumn.as(getFocalGeometryColumnName, geometryColumnMetadata)
                this.withColumn(getFocalGeometryColumnName, geometryColumnWithMetadata)
            case _ => throw MosaicSQLExceptions.BadIndexResolution(
                  mosaicContext.getIndexSystem.minResolution,
                  mosaicContext.getIndexSystem.maxResolution
                )
        }

    def getFocalGeometryColumnName: String = getFocalGeometryField.getOrElse(throw MosaicSQLExceptions.NoGeometryColumnSet).name

    def getGeometryColumn: Column = this.col(getFocalGeometryColumnName)

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

    override def withColumn(colName: String, col: Column): MosaicFrame = MosaicFrame(super.withColumn(colName, col))

    def getOptimalResolution(sampleFraction: Double): Int = {
        analyzer.getOptimalResolution(sampleFraction)
    }

    def getOptimalResolution(sampleRows: Int): Int = {
        analyzer.getOptimalResolution(sampleRows)
    }

    def getOptimalResolution: Int = {
        analyzer.getOptimalResolution(analyzer.defaultSampleFraction)
    }

    def analyzer: MosaicAnalyzer = new MosaicAnalyzer(this)

    def withPrefix(prefix: String): MosaicFrame = {
        def prepend(str: String) = s"${prefix}_$str"
        this.select(columns.map(c => col(c).alias(prepend(c))): _*)
    }

    override def select(cols: Column*): MosaicFrame = MosaicFrame(super.select(cols: _*))

    def flattenGeometries(flatGeometryColumnSuffix: String = DefaultColNames.defaultFlatGeometrySuffix): MosaicFrame = {
        val flattenedGeometryColumnName = s"${getFocalGeometryColumnName}_$flatGeometryColumnSuffix"
        val flattenedGeometryMetadata = new MetadataBuilder()
            .withMetadata(getFocalGeometryField.get.metadata)
            .remove("GeometryId")
            .remove("GeometryTypeId")
            .remove("GeometryTypeDescription")
            .build()
        this.withColumn(
          flattenedGeometryColumnName,
          flatten_polygons(getGeometryColumn).as(flattenedGeometryColumnName, flattenedGeometryMetadata)
        ).setGeometryColumn(flattenedGeometryColumnName)
    }

    def getIndexResolution: Int =
        getFocalGeometryField.getOrElse(throw MosaicSQLExceptions.NoGeometryColumnSet).metadata.getLong(ColMetaTags.INDEX_RESOLUTION).toInt

    def applyIndex(dropExistingIndexes: Boolean = true): MosaicFrame = {
        val indexId = indexCounter.getAndIncrement()
        val resolution = getIndexResolution
        // test already indexed
        val trimmedDf =
            if (isIndexed & dropExistingIndexes) {
                // this is brutal, we can refine
                this.drop(getGeometryAssociatedColumnNames(Some(indexId)): _*).distinct()
            } else {
                this
            }

        val flattenedDf =
            if (!GeometryTypeEnum.isFlat(trimmedDf.getGeometryType)) {
                trimmedDf.flattenGeometries()
            } else {
                trimmedDf
            }

        val geometryColumn = flattenedDf.getGeometryColumn
        val geometryId = flattenedDf.getGeometryId // might need this later to disambiguate joins
        val indexColumnName = auxiliaryColumnNameGen(ColRoles.INDEX, GeometryTypeEnum.groupOf(getGeometryType), geometryId, indexId)
        val chipColumnName = auxiliaryColumnNameGen(ColRoles.CHIP, GeometryTypeEnum.groupOf(getGeometryType), geometryId, indexId)
        val chipFlagColumnName = auxiliaryColumnNameGen(ColRoles.CHIP_FLAG, GeometryTypeEnum.groupOf(getGeometryType), geometryId, indexId)

        val indexedDf = (flattenedDf.getGeometryType match {
            case GeometryTypeEnum.POLYGON => MosaicFrame(
                  flattenedDf
                      .select(
                        flattenedDf.col("*"),
                        mosaic_explode(geometryColumn, resolution).as(Seq(chipFlagColumnName, indexColumnName, chipColumnName))
                      )
                )
            case GeometryTypeEnum.POINT   =>
                MosaicFrame(flattenedDf.select(flattenedDf.col("*"), point_index(geometryColumn, resolution).as(indexColumnName)))
            case _                        => flattenedDf
        })
        indexedDf.addMosaicColumnMetadata(indexId)
    }

    override def select(col: String, cols: String*): MosaicFrame = MosaicFrame(super.select(col, cols: _*))

    override def where(condition: Column): MosaicFrame = MosaicFrame(super.where(condition))

    override def limit(n: Int): MosaicFrame = MosaicFrame(super.limit(n))

    override def drop(col: Column): MosaicFrame = MosaicFrame(super.drop(col))

    override def drop(colNames: String*): MosaicFrame = MosaicFrame(super.drop(colNames: _*))

    override def distinct(): MosaicFrame = MosaicFrame(super.distinct())

    override def withColumnRenamed(existingName: String, newName: String): MosaicFrame =
        MosaicFrame(super.withColumnRenamed(existingName, newName))

    protected def defaultColName(role: String, geomGroup: GeometryTypeEnum.Value): String =
        role match {
            case ColRoles.INDEX     =>
                if (GeometryTypeEnum.polygonGeometries.contains(geomGroup)) DefaultColNames.defaultFillIndexColumnName
                else DefaultColNames.defaultPointIndexColumnName
            case ColRoles.CHIP      => DefaultColNames.defaultChipColumnName
            case ColRoles.CHIP_FLAG => DefaultColNames.defaultChipFlagColumnName
        }

    protected def auxiliaryColumnNameGen(role: String, geomGroup: GeometryTypeEnum.Value, geometryId: Long, indexId: Long): String =
        s"${defaultColName(role, geomGroup)}_${geometryId}_${indexId}"

    private def getGeometryAssociatedColumnNames(indexId: Option[Long]): List[String] = {
        for (role <- ColRoles.AUXILIARIES) yield getGeometryAssociatedFieldByRole(role, getGeometryId, indexId) match {
            case Some(f: StructField) => f.name
            case None                 => "blah"
        }
    }

    private def addMosaicColumnMetadata(indexId: Long): MosaicFrame = {
        val focalGeometryId = getFocalGeometryField.get.metadata.getLong(ColMetaTags.GEOMETRY_ID)
        val indexColumnName = auxiliaryColumnNameGen(ColRoles.INDEX, getGeometryType, focalGeometryId, indexId)
        val indexColumnMetadata = new MetadataBuilder()
            .putString(ColMetaTags.ROLE, ColRoles.INDEX)
            .putLong(ColMetaTags.INDEX_ID, indexId)
            .putString(ColMetaTags.INDEX_SYSTEM, mosaicContext.getIndexSystem.name)
            .putLong(ColMetaTags.INDEX_RESOLUTION, getIndexResolution)
            .putLong(ColMetaTags.PARENT_GEOMETRY_ID, focalGeometryId)
            .build()
        val indexColumnWithMetadata = this.col(indexColumnName).as(indexColumnName, indexColumnMetadata)
        if (GeometryTypeEnum.groupOf(getGeometryType) == GeometryTypeEnum.POLYGON) {
            val chipColumnMetadata = new MetadataBuilder()
                .putString(ColMetaTags.ROLE, ColRoles.CHIP)
                .putLong(ColMetaTags.INDEX_ID, indexId)
                .putString(ColMetaTags.GEOMETRY_ENCODING, geometryColumnEncodings(BinaryType))
                .putLong(ColMetaTags.PARENT_GEOMETRY_ID, focalGeometryId)
                .build()
            val chipFlagColumnMetadata = new MetadataBuilder()
                .putString(ColMetaTags.ROLE, ColRoles.CHIP_FLAG)
                .putLong(ColMetaTags.INDEX_ID, indexId)
                .putLong(ColMetaTags.PARENT_GEOMETRY_ID, focalGeometryId)
                .build()
            val chipColumnName = auxiliaryColumnNameGen(ColRoles.CHIP, getGeometryType, focalGeometryId, indexId)
            val chipFlagColumnName = auxiliaryColumnNameGen(ColRoles.CHIP_FLAG, getGeometryType, focalGeometryId, indexId)
            val chipColumnWithMetadata = this.col(chipColumnName).as(chipColumnName, chipColumnMetadata)
            val chipFlagColumnWithMetadata = this.col(chipFlagColumnName).as(chipFlagColumnName, chipFlagColumnMetadata)
            this
                .withColumn(indexColumnName, indexColumnWithMetadata)
                .withColumn(chipColumnName, chipColumnWithMetadata)
                .withColumn(chipFlagColumnName, chipFlagColumnWithMetadata)
        } else {
            this.withColumn(indexColumnName, indexColumnWithMetadata)
        }

    }

    override def alias(alias: String): MosaicFrame = MosaicFrame(super.alias(alias))

}

object MosaicFrame {

    //  def prettify(df: DataFrame): DataFrame = MosaicFrame(df).prettified

    protected val geometryCounter = new AtomicLong()
    protected val indexCounter = new AtomicLong()

    def apply(sparkDataFrame: DataFrame): MosaicFrame = new MosaicFrame(sparkDataFrame)

    def apply(sparkDataFrame: DataFrame, geometryCol: String): MosaicFrame = new MosaicFrame(sparkDataFrame).setGeometryColumn(geometryCol)

}
