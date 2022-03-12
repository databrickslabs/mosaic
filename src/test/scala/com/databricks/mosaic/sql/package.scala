package com.databricks.mosaic

import org.apache.spark.sql._
import org.apache.spark.sql.functions.to_json

import com.databricks.mosaic.core.geometry.api.GeometryAPI.OGC
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext

package object sql {

    // noinspection ScalaStyle
    object mocks {

        val spark: SparkSession = SparkSession.builder().getOrCreate()
        val mosaicContext: MosaicContext = MosaicContext.build(H3IndexSystem, OGC)
        import mosaicContext.functions.{st_geomfromgeojson, st_point}
        import spark.implicits._

        val polyDf: DataFrame =
            spark.read
                .json("src/test/resources/NYC_Taxi_Zones.geojson")
                .withColumn("geometry", st_geomfromgeojson(to_json($"geometry")))

        val pointDf: DataFrame =
            spark.read
                .options(
                  Map(
                    "header" -> "true",
                    "inferSchema" -> "true"
                  )
                )
                .csv("src/test/resources/nyctaxi_yellow_trips.csv")
                .withColumn("geometry", st_point($"pickup_longitude", $"pickup_latitude"))

    }

}
