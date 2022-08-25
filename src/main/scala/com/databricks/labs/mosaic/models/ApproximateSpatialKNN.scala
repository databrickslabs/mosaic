package com.databricks.labs.mosaic.models

import com.databricks.labs.mosaic.functions._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

class ApproximateSpatialKNN extends Transformer with ApproximateSpatialKNNParams {

    override val uid: String = Identifiable.randomUID("ApproximateSpatialKNN")

    private var rightDf: Dataset[_] = _

    private var mosaic: MosaicContext = _

    def setMosaic(mc: MosaicContext): Unit = mosaic = mc

    def setRightDf(df: Dataset[_]): Unit = rightDf = df

    def setKRing(k: Int): Unit = set(indexKRing, k)

    def setIndexResolution(n: Int): Unit = set(indexResolution, n)

    def setLeftFeatureCol(col: String): Unit = set(leftFeatureCol, col)

    def setRightFeatureCol(col: String): Unit = set(rightFeatureCol, col)

    def setKNeighbours(k: Int): Unit = set(kNeighbours, k)

    override def transform(dataset: Dataset[_]): DataFrame = {
        val
    }

    override def transformSchema(schema: StructType): StructType = ???

    override def copy(extra: ParamMap): ApproximateSpatialKNN = defaultCopy(extra)

    private def withKRings(df: Dataset[_], featureCol: String): Dataset[_] = {
        val mc = mosaic
        import mc.functions._
        df.withColumn(
            s"${featureCol}_chips", mosaicfill(col(featureCol), lit(get[Int](indexResolution)))
        ).withColumn(
            s"${featureCol}_chips", kring(s"${featureCol}_chips.index", lit(get[Int](indexKRing)))
        ).drop(
            s"${featureCol}_chips"
        )
    }

}
