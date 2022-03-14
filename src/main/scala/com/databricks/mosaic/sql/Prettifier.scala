package com.databricks.mosaic.sql

import java.util.Locale

import scala.util.Try

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import com.databricks.mosaic.functions.MosaicContext

object Prettifier {

    def prettifiedMosaicFrame(mosaicFrame: MosaicFrame): DataFrame = {
        prettified(mosaicFrame.toDF, Some(List(mosaicFrame.getFocalGeometryColumnName)))
    }

    def prettified(df: DataFrame, columnNames: Option[List[String]] = None): DataFrame = {
        val mosaicContext = MosaicContext.context
        import mosaicContext.functions._

        val keywords = List("WKB_", "_WKB", "_HEX", "HEX_", "COORDS_", "_COORDS", "POLYGON", "POINT", "GEOMETRY")
        val explicitColumns = columnNames.getOrElse(List())

        val casted = df.columns
            .map(colName =>
                Try {
                    if (explicitColumns.contains(colName)) {
                        st_aswkt(col(colName))
                    } else if (keywords.exists(kw => colName.toUpperCase(Locale.ROOT).contains(kw))) {
                        st_aswkt(col(colName)).alias(s"WKT($colName)")
                    } else {
                        col(colName)
                    }
                }.getOrElse(col(colName))
            )
            .toSeq

        df.select(casted: _*)
    }

}
