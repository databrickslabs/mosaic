package com.databricks.labs.mosaic.models

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.getBoroughs
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, hash}
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.file.Files

trait ApproximateSpatialKNNBehaviors {
    this: AnyFlatSpec =>

    def wktKNN(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

        val tempLocation = Files.createTempDirectory("mosaic").toAbsolutePath.toString
        val checkpointManager = CheckpointManager(tempLocation, isTable = false)

        val knn = new ApproximateSpatialKNN()
            .setIsTest(true)
            .setKNeighbours(20)
            .setLeftFeatureCol("wkt")
            .setRightFeatureCol("wkt")
            .setMaxIterations(100)
            .setEarlyStopping(3)
            // note this is CRS specific
            .setDistanceThreshold(10)
            .setIndexResolution(resolution)
            .setCheckpointTablePrefix(tempLocation)
            .setRightDf(boroughs)

        knn.transform(boroughs)
            .withColumn("left_hash", hash(col("left_wkt")))
            .withColumn("right_hash", hash(col("right_wkt")))
            .show(50)

        checkpointManager.deleteIfExists()

    }

}
