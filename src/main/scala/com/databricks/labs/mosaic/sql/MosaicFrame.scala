package com.databricks.labs.mosaic.sql

import java.util.concurrent.atomic.AtomicLong
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.sql.MosaicFrame._
import com.databricks.labs.mosaic.sql.constants._
import com.databricks.labs.mosaic.sql.join.PointInPolygonJoin
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.adapters.MosaicDataset
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.types._

class MosaicFrame(sparkDataFrame: DataFrame) extends MosaicDataset(sparkDataFrame) with Logging {

    def listGeometryColumns: List[Column] =
        this.schema.fields
            .filter(f => fieldFilter(f, Map(ColMetaTags.ROLE -> ColRoles.GEOMETRY)))
            .map(f => col(f.name))
            .toList

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

    protected def inferGeometryType(geometryColumnName: String): GeometryTypeEnum.Value = {
        val mosaicContext: MosaicContext = MosaicContext.context
        import mosaicContext.functions._
        val spark: SparkSession = this.sparkSession
        import spark.implicits._

        val geomColGeometryType = where(col(geometryColumnName).isNotNull)
            .limit(1)
            .select(st_geometrytype(col(geometryColumnName)))
            .as[String]
            .collect
            .head
        GeometryTypeEnum.fromString(geomColGeometryType)
    }

    override def where(condition: Column): MosaicFrame = MosaicFrame(super.where(condition))

    override def limit(n: Int): MosaicFrame = MosaicFrame(super.limit(n))

    override def select(cols: Column*): MosaicFrame = MosaicFrame(super.select(cols: _*))

    def getFocalGeometryColumnName: String = getFocalGeometryField.getOrElse(throw MosaicSQLExceptions.NoGeometryColumnSet).name

    override def withColumn(colName: String, col: Column): MosaicFrame = MosaicFrame(super.withColumn(colName, col))

    def getPointIndexColumn(indexId: Option[Long] = None): Column = this.col(getPointIndexColumnName(indexId))

    def getPointIndexColumnName(indexId: Option[Long]): String =
        getGeometryAssociatedFieldByRole(ColRoles.INDEX, getGeometryId, indexId) match {
            case Some(f: StructField) => f.name
            case _                    => DefaultColNames.defaultPointIndexColumnName
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

    def fieldFilter(field: StructField, tags: List[String]): Boolean = tags.forall(field.metadata.contains)

    def getFillIndexColumn(indexId: Option[Long] = None): Column = this.col(getFillIndexColumnName(indexId))

    def getFillIndexColumnName(indexId: Option[Long]): String =
        getGeometryAssociatedFieldByRole(ColRoles.INDEX, getGeometryId, indexId) match {
            case Some(f: StructField) => f.name
            case _                    => DefaultColNames.defaultFillIndexColumnName
        }

    def prettified(): DataFrame = Prettifier.prettifiedMosaicFrame(this)

    def join(other: MosaicFrame): MosaicFrame = {
        if (!this.isIndexed & GeometryTypeEnum.groupOf(getGeometryType) == GeometryTypeEnum.POLYGON) {
            throw MosaicSQLExceptions.MosaicFrameNotIndexed
        } else if (!other.isIndexed & GeometryTypeEnum.groupOf(other.getGeometryType) == GeometryTypeEnum.POLYGON) {
            throw MosaicSQLExceptions.MosaicFrameNotIndexed
        }

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

    def isIndexed: Boolean = getGeometryAssociatedFieldByRole(ColRoles.INDEX, getGeometryId, None).isDefined

    def getGeometryType: GeometryTypeEnum.Value = {
        val geomColField = getFocalGeometryField.getOrElse(throw MosaicSQLExceptions.NoGeometryColumnSet)
        val geomColGeometryType = geomColField.metadata.getLong(ColMetaTags.GEOMETRY_TYPE_ID).toInt
        GeometryTypeEnum.fromId(geomColGeometryType)
    }

    def setIndexResolution(resolution: Int): MosaicFrame = {
        val indexSystem = MosaicContext.context.getIndexSystem
        resolution match {
            case i: Int if indexSystem.resolutions contains i =>
                val geometryColumnMetadata = new MetadataBuilder()
                    .withMetadata(getFocalGeometryField.get.metadata)
                    .putLong(ColMetaTags.INDEX_RESOLUTION, i.toLong)
                    .build()
                val geometryColumnWithMetadata = getGeometryColumn.as(getFocalGeometryColumnName, geometryColumnMetadata)
                this.withColumn(getFocalGeometryColumnName, geometryColumnWithMetadata)
            case _                                            => throw MosaicSQLExceptions.BadIndexResolution(indexSystem.resolutions)
        }
    }

    def getGeometryColumn: Column = this.col(getFocalGeometryColumnName)

    def getOptimalResolution(sampleFraction: Double): Int = {
        analyzer.getOptimalResolution(sampleFraction)
    }

    def analyzer: MosaicAnalyzer = new MosaicAnalyzer(this)

    def getOptimalResolution(sampleRows: Int): Int = {
        analyzer.getOptimalResolution(SampleStrategy(sampleRows = Some(sampleRows)))
    }

    def getOptimalResolution: Int = {
        analyzer.getOptimalResolution(analyzer.defaultSampleFraction)
    }

    def indexColumnMap(resolution: Int = getIndexResolution): Map[String, Column] =
        listIndexesForGeometry()
            .filter(i => i.indexResolution == resolution)
            .maxBy(_.id)
            .indexColumnsWithRoles
            .map(i => i.role -> this.col(i.column))
            .toMap

    def listIndexesForGeometry(geometryColumnName: String = getFocalGeometryColumnName): List[MosaicFrameIndex] =
        listIndexes.filter(i => i.parentGeometryColumn == geometryColumnName)

    def listIndexes: List[MosaicFrameIndex] =
        this.schema.fields
            .filter(f => fieldFilter(f, Map(ColMetaTags.ROLE -> ColRoles.INDEX)))
            .map(f => f.metadata)
            .map(m =>
                MosaicFrameIndex(
                  m.getLong(ColMetaTags.INDEX_ID),
                  geometryColIds(m.getLong(ColMetaTags.PARENT_GEOMETRY_ID)),
                  m.getString(ColMetaTags.INDEX_SYSTEM),
                  m.getLong(ColMetaTags.INDEX_RESOLUTION).toInt,
                  indexColumns(m.getLong(ColMetaTags.INDEX_ID))
                )
            )
            .toList

    def indexColumns(indexID: Long): Seq[MosaicColumnRole] =
        this.schema.fields
            .filter(f => fieldFilter(f, Map(ColMetaTags.INDEX_ID -> indexID)))
            .groupBy(_.metadata.getString(ColMetaTags.ROLE))
            .mapValues(f => f.map(f => f.name).head)
            .map(f => MosaicColumnRole(f._1, f._2))
            .toList

    def geometryColIds: Map[Long, String] =
        this.schema.fields
            .filter(f => fieldFilter(f, List(ColMetaTags.GEOMETRY_ID)))
            .map(f => f.metadata.getLong(ColMetaTags.GEOMETRY_ID) -> f.name)
            .toMap

    def dropAllIndexes: MosaicFrame =
        listIndexes
            .filter(i => i.parentGeometryColumn == getFocalGeometryColumnName)
            .flatMap(i => i.indexColumnsWithRoles.map(_.column))
            .foldLeft(this)((m, c) => m.drop(c))
            .distinct()

    def applyIndex(dropExistingIndexes: Boolean = true, explodePolyFillIndexes: Boolean = true): MosaicFrame = {
        val mosaicContext: MosaicContext = MosaicContext.context
        import mosaicContext.functions._

        val indexId = indexCounter.getAndIncrement()
        val resolution = getIndexResolution
        // test already indexed
        val trimmedDf =
            if (isIndexed & dropExistingIndexes) {
                // this is brutal, we can refine
                dropAllIndexes
            } else {
                this
            }

        val geometryColumn = trimmedDf.getGeometryColumn
        val geometryId = trimmedDf.getGeometryId // might need this later to disambiguate joins
        val indexColumnName = auxiliaryColumnNameGen(ColRoles.INDEX, GeometryTypeEnum.groupOf(getGeometryType), geometryId, indexId)

        val indexedDf = trimmedDf.getGeometryType match {
            case GeometryTypeEnum.POLYGON | GeometryTypeEnum.MULTIPOLYGON =>
                if (explodePolyFillIndexes) trimmedDf
                    .select(
                      trimmedDf.col("*"),
                      grid_tessellateexplode(geometryColumn, resolution).as(indexColumnName)
                    )
                else trimmedDf
                    .select(
                      trimmedDf.col("*"),
                      grid_tessellate(geometryColumn, resolution).as(indexColumnName)
                    )

            case GeometryTypeEnum.POINT =>
                trimmedDf.select(trimmedDf.col("*"), grid_pointascellid(geometryColumn, resolution).as(indexColumnName))
            case _                      => trimmedDf
        }
        indexedDf.addMosaicColumnMetadata(indexId, indexColumnName, explodePolyFillIndexes)
    }

    def latestIndexId: Long =
        this.schema.fields
            .filter(f => fieldFilter(f, Map(ColMetaTags.ROLE -> ColRoles.INDEX)))
            .map(_.metadata.getLong(ColMetaTags.INDEX_ID))
            .max

    override def select(col: String, cols: String*): MosaicFrame = MosaicFrame(super.select(col, cols: _*))

    override def drop(col: Column): MosaicFrame = MosaicFrame(super.drop(col))

    override def drop(colNames: String*): MosaicFrame = MosaicFrame(super.drop(colNames: _*))

    override def distinct(): MosaicFrame = MosaicFrame(super.distinct())

    override def withColumnRenamed(existingName: String, newName: String): MosaicFrame =
        MosaicFrame(super.withColumnRenamed(existingName, newName))

    override def alias(alias: String): MosaicFrame = MosaicFrame(super.alias(alias))

    def getIndexResolution: Int =
        getFocalGeometryField.getOrElse(throw MosaicSQLExceptions.NoGeometryColumnSet).metadata.getLong(ColMetaTags.INDEX_RESOLUTION).toInt

    protected def defaultColName(role: String, geomGroup: GeometryTypeEnum.Value): String =
        role match {
            case ColRoles.INDEX     =>
                if (GeometryTypeEnum.polygonGeometries.contains(geomGroup)) DefaultColNames.defaultFillIndexColumnName
                else DefaultColNames.defaultPointIndexColumnName
            case ColRoles.CHIP      => DefaultColNames.defaultChipColumnName
            case ColRoles.CHIP_FLAG => DefaultColNames.defaultChipFlagColumnName
        }

    protected def auxiliaryColumnNameGen(role: String, geomGroup: GeometryTypeEnum.Value, geometryId: Long, indexId: Long): String =
        s"${defaultColName(role, geomGroup)}_${geometryId}_$indexId"

    private def addMosaicColumnMetadata(indexId: Long, indexColumnName: String, exploded: Boolean): MosaicFrame = {
        val mosaicContext = MosaicContext.context
        val focalGeometryId = getFocalGeometryField.get.metadata.getLong(ColMetaTags.GEOMETRY_ID)
        val mosaicColumnMetaData = new MetadataBuilder()
            .putString(ColMetaTags.ROLE, ColRoles.INDEX)
            .putLong(ColMetaTags.INDEX_ID, indexId)
            .putString(ColMetaTags.INDEX_SYSTEM, mosaicContext.getIndexSystem.name)
            .putLong(ColMetaTags.INDEX_RESOLUTION, getIndexResolution)
            .putBoolean(ColMetaTags.EXPLODED_POLYFILL, exploded)
            .putLong(ColMetaTags.PARENT_GEOMETRY_ID, focalGeometryId)
            .build()
        this.withColumn(indexColumnName, this.col(indexColumnName).as(indexColumnName, mosaicColumnMetaData))
    }

}

object MosaicFrame {

    protected val geometryCounter = new AtomicLong()
    protected val indexCounter = new AtomicLong()

    def apply(sparkDataFrame: DataFrame): MosaicFrame = new MosaicFrame(sparkDataFrame)

    def apply(sparkDataFrame: DataFrame, geometryCol: String): MosaicFrame = new MosaicFrame(sparkDataFrame).setGeometryColumn(geometryCol)

    case class MosaicColumnRole(
        role: String,
        column: String
    )

    case class MosaicFrameIndex(
        id: Long,
        parentGeometryColumn: String,
        indexSystem: String,
        indexResolution: Int,
        indexColumnsWithRoles: Seq[MosaicColumnRole]
    )

}
