package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.core.types.InternalGeometryType
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.adapters.Column
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers._

//noinspection ScalaDeprecation
trait IndexGeometryBehaviors extends MosaicSpatialQueryTest {

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        mc.register(spark)

        val indexSystem = mc.getIndexSystem
        val geometryAPIName = mc.getGeometryAPI.name

        val gridCellLong = MosaicContext.indexSystem match {
            case BNGIndexSystem => lit(1050138790L).expr
            case H3IndexSystem  => lit(623060282076758015L).expr
            case _  => lit(0L).expr
        }
        val gridCellStr = MosaicContext.indexSystem match {
            case BNGIndexSystem => lit("TQ388791").expr
            case H3IndexSystem  => lit("8a58e0682d6ffff").expr
            case _ => lit("0").expr
        }

        IndexGeometry(gridCellStr, lit("WKT").expr, indexSystem, geometryAPIName).dataType shouldEqual StringType
        IndexGeometry(gridCellStr, lit("WKB").expr, indexSystem, geometryAPIName).dataType shouldEqual BinaryType
        IndexGeometry(gridCellStr, lit("GEOJSON").expr, indexSystem, geometryAPIName).dataType shouldEqual StringType
        IndexGeometry(gridCellStr, lit("COORDS").expr, indexSystem, geometryAPIName).dataType shouldEqual InternalGeometryType
        an[Error] should be thrownBy IndexGeometry(gridCellStr, lit("BAD FORMAT").expr, indexSystem, geometryAPIName).dataType

        IndexGeometry(gridCellLong, lit("WKT").expr, indexSystem, geometryAPIName).dataType shouldEqual StringType
        IndexGeometry(gridCellLong, lit("WKB").expr, indexSystem, geometryAPIName).dataType shouldEqual BinaryType
        IndexGeometry(gridCellLong, lit("GEOJSON").expr, indexSystem, geometryAPIName).dataType shouldEqual StringType
        IndexGeometry(gridCellLong, lit("COORDS").expr, indexSystem, geometryAPIName).dataType shouldEqual InternalGeometryType
        an[Error] should be thrownBy IndexGeometry(gridCellLong, lit("BAD FORMAT").expr, indexSystem, geometryAPIName).dataType

        val longIDGeom = IndexGeometry(gridCellLong, lit("WKT").expr, indexSystem, geometryAPIName)
        val intIDGeom = IndexGeometry(Column(gridCellLong).cast(IntegerType).expr, lit("WKT").expr, indexSystem, geometryAPIName)
        val strIDGeom = IndexGeometry(gridCellStr, lit("WKT").expr, indexSystem, geometryAPIName)
        val badIDGeom = IndexGeometry(lit(true).expr, lit("WKT").expr, indexSystem, geometryAPIName)

        longIDGeom.checkInputDataTypes() shouldEqual TypeCheckResult.TypeCheckSuccess
        intIDGeom.checkInputDataTypes() shouldEqual TypeCheckResult.TypeCheckSuccess
        strIDGeom.checkInputDataTypes() shouldEqual TypeCheckResult.TypeCheckSuccess
        badIDGeom.checkInputDataTypes().isFailure shouldEqual true

        longIDGeom.makeCopy(Array(longIDGeom.left, longIDGeom.right)) shouldEqual longIDGeom

        // legacy API def tests
        MosaicContext.indexSystem match {
            case BNGIndexSystem =>
                noException should be thrownBy mc.functions.index_geometry(lit(1050138790L))
                noException should be thrownBy mc.functions.grid_boundary(lit(1050138790L), lit("WKT"))
                noException should be thrownBy mc.functions.grid_boundary(lit(1050138790L), "WKB")
            case H3IndexSystem  =>
                noException should be thrownBy mc.functions.index_geometry(lit(623060282076758015L))
                noException should be thrownBy mc.functions.grid_boundary(lit(623060282076758015L), lit("WKT"))
                noException should be thrownBy mc.functions.grid_boundary(lit(623060282076758015L), "WKB")
            case _ =>
                noException should be thrownBy mc.functions.index_geometry(lit(0L))
                noException should be thrownBy mc.functions.grid_boundary(lit(0L), lit("WKT"))
                noException should be thrownBy mc.functions.grid_boundary(lit(0L), "WKB")
        }

    }

}
