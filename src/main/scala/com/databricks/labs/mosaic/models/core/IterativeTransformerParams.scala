package com.databricks.labs.mosaic.models.core

import org.apache.spark.ml.param._

trait IterativeTransformerParams extends Params {

    val maxIterations: IntParam = new IntParam(this, "maxIterations", "Maximum number of iterations to run the algorithm.")

    val earlyStopIterations: IntParam =
        new IntParam(this, "earlyStopIterations", "Number of iterations to wait before stopping the algorithm if no new matches are found.")

    val checkpointTablePrefix: Param[String] = new Param[String](this, "checkpointTablePrefix", "Prefix for checkpoint tables.")

    def getMaxIterations: Int = $(maxIterations)

    def getEarlyStopIterations: Int = if (isDefined(earlyStopIterations)) $(earlyStopIterations) else $(maxIterations)

    def getCheckpointTablePrefix: String = $(checkpointTablePrefix)

    def setMaxIterations(value: Int): this.type = set(maxIterations, value)

    def setEarlyStopIterations(value: Int): this.type = set(earlyStopIterations, value)

    def setCheckpointTablePrefix(value: String): this.type = set(checkpointTablePrefix, value)

}
