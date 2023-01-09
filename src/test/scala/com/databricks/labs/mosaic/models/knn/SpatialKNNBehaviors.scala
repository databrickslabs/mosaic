package com.databricks.labs.mosaic.models.knn

import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.getBoroughs
import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.matchers.must.Matchers.{be, contain, noException}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.nio.file.Files

trait SpatialKNNBehaviors extends MosaicSpatialQueryTest {

    def behavior(mosaicContext: MosaicContext): Unit = {
        val mc = mosaicContext
        mc.register()
        val sc = spark
        import sc.implicits._

        val (resolution, distanceThreshold) = mc.getIndexSystem match {
            case H3IndexSystem  => (3, 100.0)
            case BNGIndexSystem => (-3, 10000.0)
        }

        val boroughs: DataFrame = getBoroughs(mc)

        val tempLocation = Files.createTempDirectory("mosaic").toAbsolutePath.toString
        spark.sparkContext.setCheckpointDir(tempLocation)
        spark.sparkContext.setLogLevel("ERROR")

        val knn = SpatialKNN(boroughs)
            .setUseTableCheckpoint(false)
            .setApproximate(false)
            .setKNeighbours(20)
            .setLandmarksFeatureCol("wkt")
            .setLandmarksRowID("landmark_id")
            .setCandidatesFeatureCol("wkt")
            .setCandidatesRowID("candidate_id")
            .setMaxIterations(100)
            .setEarlyStopIterations(3)
            // note this is CRS specific
            .setDistanceThreshold(distanceThreshold)
            .setIndexResolution(resolution)
            .setCheckpointTablePrefix(tempLocation)

        val matches = knn
            .transform(boroughs)
            .withColumn("left_hash", hash(col("wkt")))
            .withColumn("right_hash", hash(col("right_wkt")))

        matches
            .select(
              max("wkt_wkt_distance")
            )
            .as[Double]
            .collect()
            .head should be <= distanceThreshold

        matches
            .select(
              max("iteration")
            )
            .as[Int]
            .collect()
            .head should be <= 100

        matches
            .select(
              countDistinct("landmark_id")
            )
            .as[Long]
            .collect()
            .head should be(boroughs.count())

        matches
            .select(
              countDistinct("candidate_id")
            )
            .as[Long]
            .collect()
            .head should be(boroughs.count())

        matches
            .select(
              max("neighbour_number")
            )
            .as[Int]
            .collect()
            .head should be <= 20

        noException should be thrownBy knn.getParams
        noException should be thrownBy knn.getMetrics

        knn.write
            .overwrite()
            .save(s"$tempLocation/knn")

        val loadedKnn = SpatialKNN.load(s"$tempLocation/knn")

        knn.getParams should contain theSameElementsAs loadedKnn.getParams

    }

    def behaviorApproximate(mosaicContext: MosaicContext): Unit = {
        val mc = mosaicContext
        mc.register()
        val sc = spark
        import sc.implicits._

        val (resolution, distanceThreshold) = mc.getIndexSystem match {
            case H3IndexSystem  => (3, 100.0)
            case BNGIndexSystem => (-3, 10000.0)
        }

        val boroughs: DataFrame = getBoroughs(mc)

        val tempLocation = Files.createTempDirectory("mosaic").toAbsolutePath.toString
        spark.sparkContext.setCheckpointDir(tempLocation)
        spark.sparkContext.setLogLevel("ERROR")

        val knn = SpatialKNN(boroughs)
            .setUseTableCheckpoint(false)
            .setApproximate(true)
            .setKNeighbours(20)
            .setLandmarksFeatureCol("wkt")
            .setLandmarksRowID("landmark_id")
            .setCandidatesFeatureCol("wkt")
            .setCandidatesRowID("candidate_id")
            .setMaxIterations(100)
            .setEarlyStopIterations(3)
            // note this is CRS specific
            .setDistanceThreshold(distanceThreshold)
            .setIndexResolution(resolution)
            .setCheckpointTablePrefix(tempLocation)

        val matches = knn
            .transform(boroughs)
            .withColumn("left_hash", hash(col("wkt")))
            .withColumn("right_hash", hash(col("right_wkt")))

        matches
            .select(
              max("wkt_wkt_distance")
            )
            .as[Double]
            .collect()
            .head should be <= distanceThreshold

        matches
            .select(
              max("iteration")
            )
            .as[Int]
            .collect()
            .head should be <= 100

        matches
            .select(
              countDistinct("landmark_id")
            )
            .as[Long]
            .collect()
            .head should be(boroughs.count())

        matches
            .select(
              countDistinct("candidate_id")
            )
            .as[Long]
            .collect()
            .head should be(boroughs.count())

        matches
            .select(
              max("neighbour_number")
            )
            .as[Int]
            .collect()
            .head should be <= 20

        noException should be thrownBy knn.getParams
        noException should be thrownBy knn.getMetrics

        knn.write
            .overwrite()
            .save(s"$tempLocation/knn")

        val loadedKnn = SpatialKNN.read.load(s"$tempLocation/knn")

        knn.getParams should contain theSameElementsAs loadedKnn.getParams
    }

}
