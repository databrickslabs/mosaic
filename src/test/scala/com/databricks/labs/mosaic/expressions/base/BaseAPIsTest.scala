package com.databricks.labs.mosaic.expressions.base

import com.databricks.labs.mosaic.expressions.geometry.ST_Area
import com.databricks.labs.mosaic.expressions.raster.ST_BandMetaData
import org.apache.spark.sql.functions.lit
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.{be, noException}

class BaseAPIsTest extends AnyFunSuite {

    test("WithExpression Auxiliary tests") {
        noException should be thrownBy ST_BandMetaData.name
        noException should be thrownBy ST_BandMetaData.database
        noException should be thrownBy ST_BandMetaData.usage
        noException should be thrownBy ST_BandMetaData.example
        noException should be thrownBy ST_BandMetaData.group
        noException should be thrownBy ST_BandMetaData.builder
        noException should be thrownBy ST_BandMetaData.getExpressionInfo[ST_BandMetaData](None)
    }

    test("GenericExpressionFactory Auxiliary tests") {
        noException should be thrownBy GenericExpressionFactory.getBaseBuilder[ST_BandMetaData](1)
    }

}
