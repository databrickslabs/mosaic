package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ST_SRIDTest extends MosaicSpatialQueryTest with SharedSparkSession with ST_SRIDBehaviors {

    testAllGeometriesNoCodegen("Testing stSRID NO_CODEGEN") { SRIDBehaviour }
    testAllGeometriesCodegen("Testing stSRID CODEGEN_ONLY") { SRIDBehaviour }
    testAllGeometriesCodegen("Testing stSRID CODEGEN compilation") { SRIDCodegen }
    testAllGeometriesNoCodegen("Testing stSRID auxiliaryMethods") { auxiliaryMethods }

}
