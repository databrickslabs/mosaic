package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class GeometryKDiscTest extends MosaicSpatialQueryTest with SharedSparkSession with GeometryKDiscBehaviors {

    testAllNoCodegen("GeometryKDisc behavior on computed columns") { behavior }
    testAllNoCodegen("GeometryKDisc auxiliary methods") { auxiliaryMethods }

}
