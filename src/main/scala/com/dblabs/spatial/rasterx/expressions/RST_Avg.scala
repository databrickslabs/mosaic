package com.dblabs.spatial.rasterx.expressions

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.rasterx.gdal.GDALManager
import com.dblabs.spatial.rasterx.gdal.driver.{NodeFileManager, RasterDriver}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.{FunctionBuilder, expressions}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.gdal.gdal.Dataset

/** Returns the avg value per band of the raster. */
case class RST_Avg(
    tileExpr: Expression,
    expressionConfig: ExpressionConfig
) extends InvokedExpression
      with WithNewChildren {

    // Allways try to init hconf for node file manager
    NodeFileManager.init(expressionConfig.hConf)
    GDALManager.init(expressionConfig)

    private def rasterType = tileExpr.dataType.asInstanceOf[StructType].fields.head.dataType
    override def children: Seq[Expression] = Seq(tileExpr)
    override def dataType: DataType = ArrayType(DoubleType)
    override def nullable: Boolean = true
    override def prettyName: String = "rst_avg"
    override def replacement: Expression =
        rasterType match {
            case StringType => invoke(RST_Avg, "evalPath")
            case BinaryType => invoke(RST_Avg, "evalBinary")
        }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Avg extends WithExpressionInfo {

    def evalBinary(row: InternalRow): ArrayData = {
        val buffer = row.getBinary(0)
        val ds = RasterDriver.readFromBytes(buffer, Map.empty)
        val metadataRow = row.getMap(1)
        val metadata = Map[String, String](
          metadataRow
              .keyArray()
              .toSeq(StringType)
              .zip(metadataRow.valueArray().toSeq(StringType)): _*
        )
        val res = execute(ds, metadata)
        ArrayData.toArrayData(res)
    }

    def evalPath(row: InternalRow): ArrayData = {
        val path = row.getString(0)
        val ds = RasterDriver.read(path, Map.empty)
        val metadataRow = row.getMap(1)
        val metadata = Map[String, String](
          metadataRow
              .keyArray()
              .toSeq(StringType)
              .zip(metadataRow.valueArray().toSeq(StringType)): _*
        )
        val res = execute(ds, metadata)
        ArrayData.toArrayData(res)
    }

    def execute(ds: Dataset, metadata: Map[String, String]): Array[Double] = {
        (0 to ds.GetRasterCount()).map { bandIndex =>
            val band = ds.GetRasterBand(bandIndex + 1)
            if (band == null) Double.NaN
            else {
                val stats = band.AsMDArray().GetStatistics()
                if (stats == null) Double.NaN
                else stats.getMean
            }
        }.toArray
    }

    override def name: String = "rst_avg"

    override def builder(expressionConfig: ExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_Avg](1, expressionConfig)
    }

}
