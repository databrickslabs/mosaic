package com.databricks.labs.mosaic.models.knn

import org.apache.spark.ml.param._

/**
 * A trait for shared parameters for [[GridRingNeighbours]].
 * Note: miid is a shorthand for monotonically_increasing_id.
 */
trait GridRingNeighboursParams extends Params {

    val leftFeatureCol: Param[String] = new Param[String](this, "leftFeatureCol", "Column name of a column containing the geo feature.")

    val leftRowID: Param[String] = new Param[String](this, "leftRowID", "Column name of a column containing the row ID.")

    val rightFeatureCol: Param[String] = new Param[String](this, "rightFeatureCol", "Column name of a column containing the geo feature.")

    val rightRowID: Param[String] = new Param[String](this, "rightRowID", "Column name of a column containing the row ID.")

    val iterationID: IntParam = new IntParam(this, "iterationID", "Iteration ID for the current iteration of the algorithm.")

    val indexResolution: IntParam =
        new IntParam(
          this,
          "indexResolution",
          "Resolution for the index system to be used to represent geometries and to generate K rings as catchment are for neighbours."
        )

    def getLeftFeatureCol: String = $(leftFeatureCol)

    def getLeftRowID: String = if (isDefined(leftRowID)) $(leftRowID) else "left_miid"

    def getRightFeatureCol: String = $(rightFeatureCol)

    def getRightRowID: String = if (isDefined(rightRowID)) $(rightRowID) else "right_miid"

    def getIterationID: Int = $(iterationID)

    def getIndexResolution: Int = $(indexResolution)

    def setLeftFeatureCol(col: String): this.type = set(leftFeatureCol, col)

    def setLeftRowID(col: String): this.type = set(leftRowID, col)

    def setRightFeatureCol(col: String): this.type = set(rightFeatureCol, col)

    def setRightRowID(col: String): this.type = set(rightRowID, col)

    def setIndexResolution(n: Int): this.type = set(indexResolution, n)

    def setIterationID(n: Int): this.type = set(iterationID, n)

}
