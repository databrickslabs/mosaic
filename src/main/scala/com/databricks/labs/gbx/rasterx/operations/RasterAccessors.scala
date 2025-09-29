package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import org.gdal.gdal.{Dataset, gdal}

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.util.{Failure, Try}

object RasterAccessors {

    def subdatasetsMap(ds: Dataset): Map[String, String] = {
        Try(ds.GetMetadata_Dict("SUBDATASETS")) match {
            case Failure(_) => Map.empty[String, String]
            case _          =>
                val dict = ds.GetMetadata_Dict("SUBDATASETS").asScala.toMap.asInstanceOf[Map[String, String]]
                dict
        }
    }

    def memSize(ds: Dataset): Long = {
        val srcPath = ds.GetDescription()
        if (srcPath.contains("/vsimem/")) {
            gdal.GetMemFileBuffer(srcPath).length.toLong
        } else {
            Files.size(Paths.get(srcPath))
        }
    }

    def isEmpty(ds: Dataset): Boolean = {
        if (ds == null || ds.GetRasterXSize <= 0 || ds.GetRasterYSize <= 0) return true
        val n = ds.GetRasterCount; if (n <= 0) return true
        (1 to n).forall(i => BandAccessors.isEmpty(ds.GetRasterBand(i)))
    }

    def unlink(ds: Dataset): Unit = {
        // TODO: move to RasterDriver
        if (ds == null) return
        val srcPath = ds.GetDescription()
        if (srcPath.contains("/vsimem/")) {
            ds.delete() // release the dataset
            gdal.Unlink(srcPath)
        } else {
            RasterDriver.releaseDataset(ds)
        }
    }

}
