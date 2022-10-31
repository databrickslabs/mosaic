package com.databricks.labs.mosaic.models.core

import org.apache.spark.ml.param._

trait IterativeTransformerParams extends Params {

    val maxIterations: IntParam = new IntParam(this, "maxIterations", "Maximum number of iterations to run the algorithm.")

    val earlyStopping: IntParam =
        new IntParam(this, "earlyStopping", "Number of iterations to wait before stopping the algorithm if no new matches are found.")

    val checkpointTablePrefix: Param[String] = new Param[String](this, "checkpointTablePrefix", "Prefix for checkpoint tables.")

    def getMaxIterations: Int = $(maxIterations)

    def getEarlyStopping: Int = if (isDefined(earlyStopping)) $(earlyStopping) else $(maxIterations)

    def getCheckpointTablePrefix: String = $(checkpointTablePrefix)

    def setMaxIterations(value: Int): this.type = set(maxIterations, value)

    def setEarlyStopping(value: Int): this.type = set(earlyStopping, value)

    def setCheckpointTablePrefix(value: String): this.type = set(checkpointTablePrefix, value)

}
