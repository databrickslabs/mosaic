package com.databricks.labs.mosaic.core.raster.operator.pixel

import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.raster.io.RasterCleaner.dispose
import com.databricks.labs.mosaic.core.raster.operator.gdal.{GDALBuildVRT, GDALTranslate}
import com.databricks.labs.mosaic.gdal.MosaicGDAL.defaultBlockSize
import com.databricks.labs.mosaic.utils.PathUtils

import java.io.File
import scala.xml.{Elem, UnprefixedAttribute, XML}

/** MergeRasters is a helper object for merging rasters. */
object PixelCombineRasters {

    /**
      * Merges the rasters into a single raster.
      *
      * @param rasters
      *   The rasters to merge.
      * @return
      *   A MosaicRaster object.
      */
    def combine(rasters: Seq[MosaicRasterGDAL], pythonFunc: String, pythonFuncName: String): MosaicRasterGDAL = {
        val outOptions = rasters.head.getWriteOptions

        val vrtPath = PathUtils.createTmpFilePath("vrt")
        val rasterPath = PathUtils.createTmpFilePath(outOptions.extension)

        val vrtRaster = GDALBuildVRT.executeVRT(
          vrtPath,
          rasters,
          command = s"gdalbuildvrt -resolution highest"
        )
        vrtRaster.destroy()

        addPixelFunction(vrtPath, pythonFunc, pythonFuncName)

        val result = GDALTranslate.executeTranslate(
          rasterPath,
          vrtRaster.refresh(),
          command = s"gdal_translate",
          outOptions
        )

        dispose(vrtRaster)

        result
    }

    /**
      * Adds a pixel function to the VRT file. The pixel function is a Python
      * function that is applied to each pixel in the VRT file. The pixel
      * function is set for all bands in the VRT file.
      *
      * @param vrtPath
      *   The path to the VRT file.
      * @param pixFuncCode
      *   The pixel function code.
      * @param pixFuncName
      *   The pixel function name.
      */
    def addPixelFunction(vrtPath: String, pixFuncCode: String, pixFuncName: String): Unit = {
        val pixFuncTypeEl = <PixelFunctionType>{pixFuncName}</PixelFunctionType>
        val pixFuncLangEl = <PixelFunctionLanguage>Python</PixelFunctionLanguage>
        val pixFuncCodeEl = <PixelFunctionCode>
            {scala.xml.Unparsed(s"<![CDATA[$pixFuncCode]]>")}
        </PixelFunctionCode>

        val vrtContent = XML.loadFile(new File(vrtPath))
        val vrtWithPixFunc = vrtContent match {
            case body @ Elem(_, _, _, _, child @ _*) => body.copy(
                  child = child.map {
                      case el @ Elem(_, "VRTRasterBand", _, _, child @ _*) => el
                              .asInstanceOf[Elem]
                              .copy(
                                child = Seq(pixFuncTypeEl, pixFuncLangEl, pixFuncCodeEl) ++ child,
                                attributes = el
                                    .asInstanceOf[Elem]
                                    .attributes
                                    .append(
                                      new UnprefixedAttribute("subClass", "VRTDerivedRasterBand", scala.xml.Null)
                                    )
                              )
                      case el                                              => el
                  }
                )
        }

        XML.save(vrtPath, vrtWithPixFunc)

    }

}
