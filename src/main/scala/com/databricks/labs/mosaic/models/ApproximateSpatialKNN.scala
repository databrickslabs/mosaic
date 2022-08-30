package com.databricks.labs.mosaic.models

import com.databricks.labs.mosaic.functions._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructType}

class ApproximateSpatialKNN(override val uid: String) extends Transformer with ApproximateSpatialKNNParams with DefaultParamsWritable {

    private lazy val leftUUID = Identifiable.randomUID("left_dataset_mosaic_knn")
    private lazy val rightUUID = Identifiable.randomUID("right_dataset_mosaic_knn")
    private var rightDf: Dataset[_] = _
    private var mosaic: MosaicContext = _

    def this() = this(Identifiable.randomUID("approximate_spatial_knn"))

    def setMosaic(mc: MosaicContext): this.type = {
        mosaic = mc
        this
    }

    def setRightDf(df: Dataset[_]): this.type = {
        rightDf = df
        this
    }

    def setKRing(k: Int): this.type = set(indexKRing, k)

    def setIndexResolution(n: Int): this.type = set(indexResolution, n)

    def setLeftFeatureCol(col: String): this.type = set(leftFeatureCol, col)

    def setRightFeatureCol(col: String): this.type = set(rightFeatureCol, col)

    def setKNeighbours(k: Int): this.type = set(kNeighbours, k)

    override def transform(dataset: Dataset[_]): DataFrame = {
        val mc = mosaic
        import mc.functions._

        val leftCol = get(leftFeatureCol).get
        val rightCol = get(rightFeatureCol).get
        val leftDfAlias = Identifiable.randomUID("left")
        val rightDfAlias = Identifiable.randomUID("right")
        val leftKRings = withKRings(dataset.withColumn(leftUUID, monotonically_increasing_id()), leftCol).as(leftDfAlias)
        val rightKRings = withKRings(rightDf.withColumn(rightUUID, monotonically_increasing_id()), rightCol).as(rightDfAlias)

        val leftQualifiedCol = s"$leftDfAlias.$leftCol"
        val rightQualifiedCol = s"$rightDfAlias.$rightCol"

        val leftSubSchema = dataset.schema.fieldNames.map(c => col(s"$leftDfAlias.$c"))
        val rightSubSchema = rightDf.schema.fieldNames.map(c => col(s"$rightDfAlias.$c"))

        leftKRings
            .join(
              rightKRings,
              col(s"${leftQualifiedCol}_kring") === col(s"${rightQualifiedCol}_kring")
            )
            .groupBy(leftUUID)
            .agg(
              top_n_agg(
                struct(
                  st_distance(col(leftQualifiedCol), col(rightQualifiedCol)).alias(s"${leftCol}_${rightCol}_distance"),
                  struct(leftSubSchema: _*).alias("left_row"),
                  struct(rightSubSchema: _*).alias("right_row")
                ),
                get(kNeighbours).get
              )
            )
            .drop(leftUUID, rightUUID)
    }

    private def withKRings(df: Dataset[_], featureCol: String): Dataset[_] = {
        val mc = mosaic
        import mc.functions._
        val chipsCol = Identifiable.randomUID("tmp_chips_col")
        df.withColumn(
          chipsCol,
          mosaicfill(col(featureCol), lit(get(indexResolution).get))
        ).withColumn(
          s"${featureCol}_kring",
          functions.transform(
            col(s"$chipsCol.chips"),
            (el: Column) => kring(el.getItem("index_id"), get(indexKRing).get)
          )
        ).drop(
          chipsCol
        )
    }

    override def transformSchema(schema: StructType): StructType = {
        val leftCol = get(leftFeatureCol).get
        val rightCol = get(rightFeatureCol).get
        val newSchema = new StructType()
        newSchema.add(s"${leftCol}_${rightCol}_distance", DoubleType)
        schema.fields.foreach(newSchema.add)
        rightDf.schema.fields.foreach(newSchema.add)
        newSchema
    }

    override def copy(extra: ParamMap): ApproximateSpatialKNN = defaultCopy(extra)

}
