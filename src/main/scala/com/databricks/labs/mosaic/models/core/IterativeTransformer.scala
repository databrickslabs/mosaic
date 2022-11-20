package com.databricks.labs.mosaic.models.core

import org.apache.spark.internal.Logging
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Base class for iterative transformers. For specific behaviour, override the
  * following methods: <ul> <li> inputTransform (optional) - transform the input
  * dataset before the first iteration, default is noop. </li> <li>
  * iterationTransform - transformation to be applied each iteration. </li> <li>
  * earlyStoppingCheck (optional) - default implementation always returns false
  * (no early stopping). </li> <li> outputTransform (optional) - transform the
  * result dataset after the last iteration, default is noop. </ul> </ul>
  */
trait IterativeTransformer extends Transformer with IterativeTransformerParams with Logging {

    /**
      * Transform the input dataset before the first iteration. Default
      * implementation is a noop.
      *
      * @param input
      *   Input dataset
      * @return
      *   Transformed dataset
      */
    def inputTransform(input: Dataset[_]): DataFrame = input.toDF()

    /**
      * Transform the result dataset after the last iteration. Default
      * implementation is a noop.
      *
      * @param dataset
      *   Result dataset
      * @return
      *   Transformed dataset
      */
    def resultTransform(dataset: Dataset[_]): DataFrame = dataset.toDF()

    /**
      * Transform the dataset for each iteration using iterationTransform. The
      * input schema must match the output schema of iterationTransform. The
      * output schema must match the input schema of iterationTransform.
      * @param dataset
      *   Input dataset
      * @return
      *   Transformed dataset
      */
    def iterate(dataset: Dataset[_]): DataFrame = {

        val initialInput = inputTransform(dataset).checkpoint(true)

        logInfo(s"Starting iterative transformation with $getMaxIterations iterations.")

        // 1 based iteration counter
        val (_, _, _, result) = (1 to getMaxIterations)
            .foldLeft((0, false, false, initialInput)) {
                case ((earlyStoppingCount, previousEarlyStoppingCondition, stop, iterationInput), iteration) =>
                    if (!stop) {
                        logInfo(
                          s"Starting iteration $iteration, early stopping count: $earlyStoppingCount, previous early stopping condition: $previousEarlyStoppingCondition, stop: $stop"
                        )

                        val iterationOutput = iterationTransform(iterationInput).checkpoint(true)

                        val earlyStoppingCondition = earlyStoppingCheck(iterationInput, iterationOutput)

                        val newEarlyStoppingCount =
                            if (earlyStoppingCondition != previousEarlyStoppingCondition) 0
                            else earlyStoppingCount + 1

                        if (earlyStoppingCondition && (newEarlyStoppingCount == getEarlyStopIterations)) {
                            logInfo(s"Stopping iteration $iteration. No change in dataset.")
                            (newEarlyStoppingCount, earlyStoppingCondition, true, iterationOutput)
                        } else {
                            (newEarlyStoppingCount, earlyStoppingCondition, false, iterationOutput)
                        }
                    } else {
                        (earlyStoppingCount, previousEarlyStoppingCondition, stop, iterationInput)
                    }
            }

        resultTransform(result).checkpoint(true)
    }

    // noinspection ScalaUnusedSymbol
    /**
      * Check if early stopping condition is met. Default implementation always
      * returns false (no early stopping).
      *
      * @param preDf
      *   Input dataset
      * @param postDf
      *   Output dataset
      * @return
      *   True if early stopping condition is met, false otherwise
      */
    def earlyStoppingCheck(preDf: Dataset[_], postDf: Dataset[_]): Boolean = false

    /**
      * Transformation to be applied each iteration.
      *
      * @param dataset
      *   Input dataset
      * @return
      *   Transformed dataset
      */
    def iterationTransform(dataset: Dataset[_]): DataFrame

}
