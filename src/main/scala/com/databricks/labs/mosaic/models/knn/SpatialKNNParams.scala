package com.databricks.labs.mosaic.models.knn

import com.databricks.labs.mosaic.models.core.IterativeTransformerParams
import org.apache.spark.ml.param._

trait SpatialKNNParams extends Params with IterativeTransformerParams {

    val useTableCheckpoint: BooleanParam =
        new BooleanParam(this, "useTableCheckpoint", "Use a table checkpoint to store intermediate results.")

    val approximate: BooleanParam =
        new BooleanParam(this, "approximate", "Whether to use approximation that runs faster or to ensure exactness.")

    val landmarksFeatureCol: Param[String] =
        new Param[String](this, "landmarksFeatureCol", "Column name of a column containing the landmarks geo feature.")

    val landmarksRowID: Param[String] =
        new Param[String](this, "landmarksRowID", "Column name of a column containing the landmarks row ID.")

    val candidatesFeatureCol: Param[String] =
        new Param[String](this, "candidatesFeatureCol", "Column name of a column containing the candidates geo feature.")

    val candidatesRowID: Param[String] =
        new Param[String](this, "candidatesRowID", "Column name of a column containing the candidates row ID.")

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
          "Number defining how many neighbours will be returned for each landmark feature from candidates features."
        )

    def getUseTableCheckpoint: Boolean = $(useTableCheckpoint)

    def getApproximate: Boolean = $(approximate)

    def getLandmarksFeatureCol: String = $(landmarksFeatureCol)

    def getLandmarksRowID: String = if (isDefined(landmarksRowID)) $(landmarksRowID) else "landmarks_miid"

    def getCandidatesFeatureCol: String = $(candidatesFeatureCol)

    def getCandidatesRowID: String = if (isDefined(candidatesRowID)) $(candidatesRowID) else "candidates_miid"

    // If no distance threshold is set, we will use a very large number that is outside of
    // the bounds of any CRS being used.
    def getDistanceThreshold: Double = if (isDefined(distanceThreshold)) $(distanceThreshold) else Double.MaxValue

    def getIndexResolution: Int = $(indexResolution)

    def getKNeighbours: Int = $(kNeighbours)

    def setUseTableCheckpoint(b: Boolean): this.type = set(useTableCheckpoint, b)

    def setApproximate(b: Boolean): this.type = set(approximate, b)

    def setLandmarksFeatureCol(col: String): this.type = set(landmarksFeatureCol, col)

    def setLandmarksRowID(col: String): this.type = set(landmarksRowID, col)

    def setCandidatesFeatureCol(col: String): this.type = set(candidatesFeatureCol, col)

    def setCandidatesRowID(col: String): this.type = set(candidatesRowID, col)

    def setDistanceThreshold(n: Double): this.type = set(distanceThreshold, n)

    def setIndexResolution(n: Int): this.type = set(indexResolution, n)

    def setKNeighbours(k: Int): this.type = set(kNeighbours, k)

}
