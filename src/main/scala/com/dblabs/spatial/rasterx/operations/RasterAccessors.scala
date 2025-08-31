package com.dblabs.spatial.rasterx.operations

import com.dblabs.spatial.rasterx.gdal.RasterDriver
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants.GA_ReadOnly

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.util.{Failure, Try}

object RasterAccessors {

    def getSubdataset(name: String) = {}

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
        ds.GetRasterYSize() == 0 && ds.GetRasterXSize() == 0 || {
            val bandCount = ds.GetRasterCount()
            if (bandCount == 0) {
                val subdss = subdatasetsMap(ds)
                subdss.isEmpty || subdss.values.forall { subdsName =>
                    val srcPath = ds.GetDescription()
                    val driver = ds.GetDriver().getShortName
                    val readPath = s"$driver:$srcPath:$subdsName"
                    val sds = gdal.Open(readPath, GA_ReadOnly)
                    val n = sds.GetRasterCount()
                    if (n == 0) true
                    else {
                        (1 to n)
                            .takeWhile(i => {
                                val band = sds.GetRasterBand(i)
                                band.AsMDArray().GetStatistics().getValid_count == 0
                            })
                            .isEmpty
                    }
                }
            } else {
                (0 until bandCount)
                    .takeWhile(i => {
                        val band = ds.GetRasterBand(i + 1)
                        band.AsMDArray().GetStatistics().getValid_count > 0
                    })
                    .isEmpty
            }
        }
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
