package com.databricks.labs.mosaic.models.knn

import com.databricks.labs.mosaic.models.core.IterativeTransformerParams
import org.apache.spark.ml.param._

trait ApproximateSpatialKNNParams extends Params with IterativeTransformerParams {

    val useTableCheckpoint: BooleanParam =
        new BooleanParam(this, "useTableCheckpoint", "Use a table checkpoint to store intermediate results.")

    val leftFeatureCol: Param[String] = new Param[String](this, "leftFeatureCol", "Column name of a column containing the geo feature.")

    val leftRowID: Param[String] = new Param[String](this, "leftRowID", "Column name of a column containing the row ID.")

    val rightFeatureCol: Param[String] = new Param[String](this, "rightFeatureCol", "Column name of a column containing the geo feature.")

    val rightRowID: Param[String] = new Param[String](this, "rightRowID", "Column name of a column containing the row ID.")

    val distanceThreshold: DoubleParam = new DoubleParam(this, "distanceThreshold", "Distance threshold to stop the algorithm.")

    val indexResolution: IntParam =
        new IntParam(
          this,
          "indexResolution",
          "Resolution for the index system to be used to represent geometries and to generate K rings as catchment are for neighbours."
        )

    val kNeighbours: IntParam =
        new IntParam(
          this,
          "kNeighbours",
          "Number defining how many neighbours will be returned for each left feature from right feature."
        )

    def getUseTableCheckpoint: Boolean = $(useTableCheckpoint)

    def getLeftFeatureCol: String = $(leftFeatureCol)

    def getLeftRowID: String = if (isDefined(leftRowID)) $(leftRowID) else "left_miid"

    def getRightFeatureCol: String = $(rightFeatureCol)

    def getRightRowID: String = if (isDefined(rightRowID)) $(rightRowID) else "right_miid"

    // If no distance threshold is set, we will use a very large number that is outside of
    // the bounds of any CRS being used.
    def getDistanceThreshold: Double = if (isDefined(distanceThreshold)) $(distanceThreshold) else Double.MaxValue

    def getIndexResolution: Int = $(indexResolution)

    def getKNeighbours: Int = $(kNeighbours)

    def setUseTableCheckpoint(b: Boolean): this.type = set(useTableCheckpoint, b)

    def setLeftFeatureCol(col: String): this.type = set(leftFeatureCol, col)

    def setLeftRowID(col: String): this.type = set(leftRowID, col)

    def setRightFeatureCol(col: String): this.type = set(rightFeatureCol, col)

    def setRightRowID(col: String): this.type = set(rightRowID, col)

    def setDistanceThreshold(n: Double): this.type = set(distanceThreshold, n)

    def setIndexResolution(n: Int): this.type = set(indexResolution, n)

    def setKNeighbours(k: Int): this.type = set(kNeighbours, k)

}
