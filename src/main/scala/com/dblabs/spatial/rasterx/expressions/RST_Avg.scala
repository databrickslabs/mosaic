package com.dblabs.spatial.rasterx.expressions

import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALInfo
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.GenericExpressionFactory
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import com.dblabs.spatial.expressions.{ExpressionConfig, GenericExpressionFactory, InvokedExpression, WithExpressionInfo, WithNewChildren}
import com.dblabs.spatial.gridx.bng.BNG_AsWKB
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** Returns the avg value per band of the raster. */
case class RST_Avg(
    tileExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends InvokedExpression
    with WithNewChildren {

    override def children: Seq[Expression] = Seq(tileExpr)
    override def dataType: DataType = ArrayType(DoubleType)
    override def nullable: Boolean = true
    override def prettyName: String = "rst_avg"
    override def replacement: Expression = invoke(RST_Avg)


    /** Returns the avg value per band of the raster. */
    override def rasterTransform(tile: MosaicRasterTile): Any = {
        import org.json4s._
        import org.json4s.jackson.JsonMethods._
        implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

        val command = s"gdalinfo -stats -json -mm -nogcp -nomd -norat -noct"
        val gdalInfo = GDALInfo.executeInfo(tile.raster, command)
        // parse json from gdalinfo
        val json = parse(gdalInfo).extract[Map[String, Any]]
        val meanValues =
            json("bands").asInstanceOf[List[Map[String, Any]]].map { band => band.getOrElse("mean", Double.NaN).asInstanceOf[Double] }
        ArrayData.toArrayData(meanValues.toArray)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Avg extends WithExpressionInfo {

    def eval(path: UTF8String): ArrayData = {

    }

    def execute(path: String): Array[Double] = {
        val ds = GDAL
    }

    override def name: String = "rst_avg"

    override def builder(expressionConfig: ExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_Avg](1, expressionConfig)
    }

}
