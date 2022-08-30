package com.databricks.labs.mosaic.models

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.getBoroughs
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

trait ApproximateSpatialKNNBehaviors {
    this: AnyFlatSpec =>

    def wktKNN(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

        val knn = new ApproximateSpatialKNN()
            .setKNeighbours(10)
            .setLeftFeatureCol("wkt")
            .setRightFeatureCol("wkt")
            .setIndexResolution(1)
            .setKRing(5)
            .setMosaic(mc)
            .setRightDf(boroughs)

        knn.transform(boroughs).show(50, truncate = false)

    }

}
