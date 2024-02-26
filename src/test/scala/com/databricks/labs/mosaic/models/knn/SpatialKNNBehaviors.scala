package com.databricks.labs.mosaic.models.knn

import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, CustomIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.getBoroughs
import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.{be, contain, noException}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.nio.file.Files

trait SpatialKNNBehaviors { this: AnyFlatSpec =>

    def noApproximation(mosaicContext: MosaicContext, spark: SparkSession): Unit = {
        val mc = mosaicContext
        mc.register()
        val sc = spark
        import sc.implicits._

        val (resolution, distanceThreshold) = mc.getIndexSystem match {
            case H3IndexSystem  => (3, 100.0)
            case BNGIndexSystem => (-3, 10000.0)
            case CustomIndexSystem(_) => (3, 10000.0)
            case _ => (3, 100.0)
        }

        val boroughs: DataFrame = getBoroughs(mc)

        val tempLocation = MosaicContext.tmpDir(null)
        spark.sparkContext.setCheckpointDir(tempLocation)
        spark.sparkContext.setLogLevel("ERROR")

        val knn = SpatialKNN(boroughs)
            .setUseTableCheckpoint(false)
            .setApproximate(false)
            .setKNeighbours(5)
            .setLandmarksFeatureCol("wkt")
            .setLandmarksRowID("landmark_id")
            .setCandidatesFeatureCol("wkt")
            .setCandidatesRowID("candidate_id")
            .setMaxIterations(10)
            .setEarlyStopIterations(3)
            // note this is CRS specific
            .setDistanceThreshold(distanceThreshold)
            .setIndexResolution(resolution)
            .setCheckpointTablePrefix(tempLocation)

        val matches = knn
            .transform(boroughs)
            .withColumn("left_hash", hash(col("wkt")))
            .withColumn("right_hash", hash(col("right_wkt")))
            .select("wkt_wkt_distance", "iteration", "landmark_id", "candidate_id", "neighbour_number")
            .collect()

        matches.map(r => r.getDouble(0)).max should be <= distanceThreshold // wkt_wkt_distance
        matches.map(r => r.getInt(1)).max should be <= 10 // iteration
        matches.map(r => r.getLong(2)).distinct.length should be(boroughs.count()) // landmarks_miid
        matches.map(r => r.getLong(3)).distinct.length should be(boroughs.count()) // candidates_miid
        matches.map(r => r.getInt(4)).max should be  <= 5 // neighbour_number

        noException should be thrownBy knn.getParams
        noException should be thrownBy knn.getMetrics

        knn.write
            .overwrite()
            .save(s"$tempLocation/knn")

        val loadedKnn = SpatialKNN.load(s"$tempLocation/knn")

        knn.getParams should contain theSameElementsAs loadedKnn.getParams

    }

    def behaviorApproximate(mosaicContext: MosaicContext, spark: SparkSession): Unit = {
        val mc = mosaicContext
        mc.register()
        val sc = spark
        import sc.implicits._

        val (resolution, distanceThreshold) = mc.getIndexSystem match {
            case H3IndexSystem  => (3, 100.0)
            case BNGIndexSystem => (-3, 10000.0)
            case _ => (3, 100.0)
        }

        if (mc.getIndexSystem.name.startsWith("CUSTOM")) {
            // Skip the KNN tests for custom grid
            // TODO: Fix this
            return
        }

        val boroughs: DataFrame = getBoroughs(mc)

        val tempLocation = MosaicContext.tmpDir(null)
        spark.sparkContext.setCheckpointDir(tempLocation)
        spark.sparkContext.setLogLevel("ERROR")

        val knn = SpatialKNN(boroughs)
            .setUseTableCheckpoint(false)
            .setApproximate(true)
            .setKNeighbours(5)
            .setLandmarksFeatureCol("wkt")
            .setLandmarksRowID("landmark_id")
            .setCandidatesFeatureCol("wkt")
            .setCandidatesRowID("candidate_id")
            .setMaxIterations(10)
            .setEarlyStopIterations(3)
            // note this is CRS specific
            .setDistanceThreshold(distanceThreshold)
            .setIndexResolution(resolution)
            .setCheckpointTablePrefix(tempLocation)

        val matches = knn
            .transform(boroughs)
            .withColumn("left_hash", hash(col("wkt")))
            .withColumn("right_hash", hash(col("right_wkt")))
            .select("wkt_wkt_distance", "iteration", "landmark_id", "candidate_id", "neighbour_number")
            .collect()

        matches.map(r => r.getDouble(0)).max should be <= distanceThreshold // wkt_wkt_distance
        matches.map(r => r.getInt(1)).max should be <= 10 // iteration
        matches.map(r => r.getLong(2)).distinct.length should be(boroughs.count()) // landmarks_miid
        matches.map(r => r.getLong(3)).distinct.length should be(boroughs.count()) // candidates_miid
        matches.map(r => r.getInt(4)).max should be  <= 5 // neighbour_number

        noException should be thrownBy knn.getParams
        noException should be thrownBy knn.getMetrics

        knn.write
            .overwrite()
            .save(s"$tempLocation/knn")

        val loadedKnn = SpatialKNN.read.load(s"$tempLocation/knn")

        knn.getParams should contain theSameElementsAs loadedKnn.getParams
    }

}
