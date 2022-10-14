package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.core.types.InternalGeometryType
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.adapters.Column
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

trait IndexGeometryBehaviors {
    this: AnyFlatSpec =>

    def auxiliaryMethods(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        mosaicContext.register(spark)

        val indexSystemName = mosaicContext.getIndexSystem.name
        val geometryAPIName = mosaicContext.getGeometryAPI.name

        val gridCellLong = MosaicContext.indexSystem match {
            case BNGIndexSystem => lit(1050138790L).expr
            case H3IndexSystem  => lit(623060282076758015L).expr
        }
        val gridCellStr = MosaicContext.indexSystem match {
            case BNGIndexSystem => lit("TQ388791").expr
            case H3IndexSystem  => lit("8a58e0682d6ffff").expr
        }

        IndexGeometry(gridCellStr, lit("WKT").expr, indexSystemName, geometryAPIName).dataType shouldEqual StringType
        IndexGeometry(gridCellStr, lit("WKB").expr, indexSystemName, geometryAPIName).dataType shouldEqual BinaryType
        IndexGeometry(gridCellStr, lit("GEOJSON").expr, indexSystemName, geometryAPIName).dataType shouldEqual StringType
        IndexGeometry(gridCellStr, lit("COORDS").expr, indexSystemName, geometryAPIName).dataType shouldEqual InternalGeometryType
        an[Error] should be thrownBy IndexGeometry(gridCellStr, lit("BAD FORMAT").expr, indexSystemName, geometryAPIName).dataType

        IndexGeometry(gridCellLong, lit("WKT").expr, indexSystemName, geometryAPIName).dataType shouldEqual StringType
        IndexGeometry(gridCellLong, lit("WKB").expr, indexSystemName, geometryAPIName).dataType shouldEqual BinaryType
        IndexGeometry(gridCellLong, lit("GEOJSON").expr, indexSystemName, geometryAPIName).dataType shouldEqual StringType
        IndexGeometry(gridCellLong, lit("COORDS").expr, indexSystemName, geometryAPIName).dataType shouldEqual InternalGeometryType
        an[Error] should be thrownBy IndexGeometry(gridCellLong, lit("BAD FORMAT").expr, indexSystemName, geometryAPIName).dataType

        val longIDGeom = IndexGeometry(gridCellLong, lit("WKT").expr, indexSystemName, geometryAPIName)
        val intIDGeom = IndexGeometry(Column(gridCellLong).cast(IntegerType).expr, lit("WKT").expr, indexSystemName, geometryAPIName)
        val strIDGeom = IndexGeometry(gridCellStr, lit("WKT").expr, indexSystemName, geometryAPIName)
        val badIDGeom = IndexGeometry(lit(true).expr, lit("WKT").expr, indexSystemName, geometryAPIName)

        longIDGeom.checkInputDataTypes() shouldEqual TypeCheckResult.TypeCheckSuccess
        intIDGeom.checkInputDataTypes() shouldEqual TypeCheckResult.TypeCheckSuccess
        strIDGeom.checkInputDataTypes() shouldEqual TypeCheckResult.TypeCheckSuccess
        badIDGeom.checkInputDataTypes().isFailure shouldEqual true

        longIDGeom.makeCopy(Array(longIDGeom.left, longIDGeom.right)) shouldEqual longIDGeom

        // legacy API def tests
        MosaicContext.indexSystem match {
            case BNGIndexSystem => noException should be thrownBy mc.functions.index_geometry(lit(1050138790L))
            case H3IndexSystem  => noException should be thrownBy mc.functions.index_geometry(lit(623060282076758015L))
        }
    }

}
