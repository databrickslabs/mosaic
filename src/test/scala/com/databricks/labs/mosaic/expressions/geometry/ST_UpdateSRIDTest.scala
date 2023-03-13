package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ST_UpdateSRIDTest extends MosaicSpatialQueryTest with SharedSparkSession with ST_UpdateSRIDBehaviors {

    testAllNoCodegen("Testing stUpdateSRID") { updateSRIDBehaviour }
    testAllCodegen("Testing stUpdateSRID") { updateSRIDBehaviour }
    testAllNoCodegen("Testing stUpdateSRID auxiliaryMethods") { auxiliaryMethods }

}
