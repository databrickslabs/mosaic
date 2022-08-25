package com.databricks.labs.mosaic.models

import org.apache.spark.ml.param._

trait ApproximateSpatialKNNParams extends Params {

    val leftFeatureCol: Param[String] = new Param[String](this, "leftFeatureCol", "Column name of a column containing the geo feature.")

    val rightFeatureCol: Param[String] = new Param[String](this, "rightFeatureCol", "Column name of a column containing the geo feature.")

    val indexKRing: IntParam =
        new IntParam(
          this,
          "indexKRing",
          "K ring to be used to approximate spatial distance. Only features falling into the K ring will be considered as candidates for KNN output."
        )

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

    setDefault(indexKRing -> 3, indexResolution -> 8)

}
