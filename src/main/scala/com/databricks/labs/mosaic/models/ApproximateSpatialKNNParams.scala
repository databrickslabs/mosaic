package com.databricks.labs.mosaic.models

import org.apache.spark.ml.param._

trait ApproximateSpatialKNNParams extends Params {

    val isTest: BooleanParam = new BooleanParam(this, "isTest", "Flag used for testing to avoid writing to tables.")

    val leftFeatureCol: Param[String] = new Param[String](this, "leftFeatureCol", "Column name of a column containing the geo feature.")

    val rightFeatureCol: Param[String] = new Param[String](this, "rightFeatureCol", "Column name of a column containing the geo feature.")

    val maxIterations: Param[Int] = new Param[Int](this, "maxIterations", "Maximum number of iterations to run the algorithm.")

    val earlyStopping: Param[Int] = new Param[Int](this, "earlyStopping", "Number of iterations to wait before stopping the algorithm if no new matches are found.")

    val distanceThreshold: Param[Double] = new Param[Double](this, "distanceThreshold", "Distance threshold to stop the algorithm.")

    val checkpointTablePrefix: Param[String] = new Param[String](this, "checkpointTablePrefix", "Prefix for checkpoint tables.")

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

    def getIsTest: Boolean = if (isDefined(isTest)) $(isTest) else false

    def getLeftFeatureCol: String = $(leftFeatureCol)

    def getRightFeatureCol: String = $(rightFeatureCol)

    // 20 iterations in K space is a lot. We are selecting resolution based on the
    // average number of geometries per cell in the right hand side dataset.
    def getMaxIterations: Int = if (isDefined(maxIterations)) $(maxIterations) else 20

    def getEarlyStopping: Int = if (isDefined(earlyStopping)) $(earlyStopping) else $(maxIterations)

    // If no distance threshold is set, we will use a very large number that is outside of
    // the bounds of any CRS being used.
    def getDistanceThreshold: Double = if (isDefined(distanceThreshold)) $(distanceThreshold) else Double.MaxValue

    def getCheckpointTablePrefix: String = $(checkpointTablePrefix)

    def getIndexResolution: Int = $(indexResolution)

    def getKNeighbours: Int = $(kNeighbours)

    def setIsTest(value: Boolean): this.type = set(isTest, value)

    def setLeftFeatureCol(col: String): this.type = set(leftFeatureCol, col)

    def setRightFeatureCol(col: String): this.type = set(rightFeatureCol, col)

    def setMaxIterations(n: Int): this.type = set(maxIterations, n)

    def setEarlyStopping(n: Int): this.type = set(earlyStopping, n)

    def setDistanceThreshold(n: Double): this.type = set(distanceThreshold, n)

    def setCheckpointTablePrefix(prefix: String): this.type = set(checkpointTablePrefix, prefix)

    def setIndexResolution(n: Int): this.type = set(indexResolution, n)

    def setKNeighbours(k: Int): this.type = set(kNeighbours, k)

}
