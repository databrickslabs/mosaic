package com.databricks.labs.mosaic.core.io

import com.databricks.labs.mosaic.sql.MosaicFrameBehaviors
import com.databricks.labs.mosaic.test.SparkSuite
import org.scalatest.flatspec.AnyFlatSpec

class GeoTiffReaderTest extends AnyFlatSpec with GeoTiffBehaviours with SparkSuite {
    "GeoTiff" should "read an image from a stored byte array" in {
        it should behave like testImageFromSparkBytes(spark)
    }
}
