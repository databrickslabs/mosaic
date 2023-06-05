package com.databricks.labs.mosaic.core.raster.gdal_raster

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster._
import com.databricks.labs.mosaic.core.raster.operator.RasterClipByVector
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.POLYGON
import org.apache.orc.util.Murmur3
import org.gdal.gdal.gdal.GDALInfo
import org.gdal.gdal.{Dataset, InfoOptions, gdal}
import org.gdal.gdalconst.gdalconstConstants._
import org.gdal.osr
import org.gdal.osr.SpatialReference
import org.locationtech.proj4j.CRSFactory

import java.nio.file.{Files, Paths}
import java.util
import java.util.{Locale, UUID, Vector => JVector}
import scala.collection.JavaConverters.dictionaryAsScalaMapConverter
import scala.util.Try

/** GDAL implementation of the MosaicRaster trait. */
//noinspection DuplicatedCode
class MosaicRasterGDAL(_uuid: Long, raster: Dataset, path: String) extends MosaicRaster(isInMem = false) {

    protected val crsFactory: CRSFactory = new CRSFactory

    private val wsg84 = new osr.SpatialReference()
    wsg84.ImportFromEPSG(4326)

    override def metadata: Map[String, String] = {
        Option(raster.GetMetadataDomainList())
            .map(_.toArray)
            .map(domain =>
                domain
                    .map(domainName =>
                        Option(raster.GetMetadata_Dict(domainName.toString))
                            .map(_.asScala.toMap.asInstanceOf[Map[String, String]])
                            .getOrElse(Map.empty[String, String])
                    )
                    .reduceOption(_ ++ _)
                    .getOrElse(Map.empty[String, String])
            )
            .getOrElse(Map.empty[String, String])

    }

    override def subdatasets: Map[String, String] = {
        val subdatasetsMap = Option(raster.GetMetadata_Dict("SUBDATASETS"))
            .map(_.asScala.toMap.asInstanceOf[Map[String, String]])
            .getOrElse(Map.empty[String, String])
        val keys = subdatasetsMap.keySet
        keys.flatMap(key =>
            if (key.toUpperCase(Locale.ROOT).contains("NAME")) {
                val path = subdatasetsMap(key)
                Seq(
                  key -> path.split(":").last,
                  path.split(":").last -> path
                )
            } else Seq(key -> subdatasetsMap(key))
        ).toMap
    }

    override def SRID: Int = {
        Try(crsFactory.readEpsgFromParameters(proj4String))
            .filter(_ != null)
            .getOrElse("EPSG:0")
            .split(":")
            .last
            .toInt
    }

    override def proj4String: String = Try(raster.GetSpatialRef.ExportToProj4).filter(_ != null).getOrElse("")

    override def getBand(bandId: Int): MosaicRasterBand = {
        if (bandId > 0 && numBands >= bandId) {
            MosaicRasterBandGDAL(raster.GetRasterBand(bandId), bandId)
        } else {
            throw new ArrayIndexOutOfBoundsException()
        }
    }

    override def numBands: Int = raster.GetRasterCount()

    // noinspection ZeroIndexToHead
    override def extent: Seq[Double] = {
        val minX = getGeoTransform(0)
        val maxY = getGeoTransform(3)
        val maxX = minX + getGeoTransform(1) * xSize
        val minY = maxY + getGeoTransform(5) * ySize
        Seq(minX, minY, maxX, maxY)
    }

    override def xSize: Int = raster.GetRasterXSize

    override def ySize: Int = raster.GetRasterYSize

    override def getGeoTransform: Array[Double] = raster.GetGeoTransform()

    override def getRaster: Dataset = this.raster

    def spatialRef: SpatialReference = raster.GetSpatialRef()

    override def transformBands[T](f: MosaicRasterBand => T): Seq[T] = for (i <- 1 to numBands) yield f(getBand(i))

    /**
      * @return
      *   Returns MosaicGeometry representing bounding box of the raster.
      */
    override def bbox(geometryAPI: GeometryAPI, destCRS: SpatialReference = wsg84): MosaicGeometry = {
        val gt = getGeoTransform

        val sourceCRS = spatialRef
        val transform = new osr.CoordinateTransformation(sourceCRS, destCRS)

        val bbox = geometryAPI.geometry(
          Seq(
            (gt(0), gt(3)),
            (gt(0) + gt(1) * xSize, gt(3)),
            (gt(0) + gt(1) * xSize, gt(3) + gt(5) * ySize),
            (gt(0), gt(3) + gt(5) * ySize)
          ).map(t => {
              // TransformPoint returns (x, y, z) coordinates
              // We dont need the z coordinate
              val p = transform.TransformPoint(t._1, t._2).dropRight(1)
              // GDAL rasters are stored upside down
              // We need to reverse the order of the coordinates
              geometryAPI.fromCoords(p.toSeq.reverse)
          }),
          POLYGON
        )

        bbox
    }

    def isEmpty: Boolean = {
        import org.json4s._
        import org.json4s.jackson.JsonMethods._
        implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

        val vector = new JVector[String]()
        vector.add("-stats")
        vector.add("-json")
        val infoOptions = new InfoOptions(vector)
        val gdalInfo = GDALInfo(getRaster, infoOptions)
        val json = parse(gdalInfo).extract[Map[String, Any]]

        json("STATISTICS_VALID_PERCENT").asInstanceOf[Double] == 0.0
    }

    override def getPath: String = "path"

    override def getRasterForCell(cellID: Long, indexSystem: IndexSystem, geometryAPI: GeometryAPI): MosaicRaster = {
        val cellGeom = indexSystem.indexToGeometry(cellID, geometryAPI)
        val swappedCellGeom = geometryAPI.geometry(
          cellGeom.getShellPoints.head.map(p => geometryAPI.fromCoords(Seq(p.getY, p.getX))),
          POLYGON
        )
        RasterClipByVector.clip(this, swappedCellGeom, geometryAPI, reproject = true)
    }

    /**
      * Cleans up the raster driver and references.
      *
      * Unlinks the raster file. After this operation the raster object is no
      * longer usable. To be used as last step in expression after writing to
      * bytes.
      */
    override def cleanUp(): Unit = {
        val isInMem = path.startsWith("/vsimem/")
        if (isInMem) {
            gdal.Unlink(path)
        }
    }

    /** @return Returns the amount of memory occupied by the file in bytes. */
    override def getMemSize: Long = writeToBytes().length

    /**
      * Writes a raster to a file system path.
      *
      * @param path
      *   The path to the raster file.
      * @return
      *   A boolean indicating if the write was successful.
      */
    override def writeToPath(path: String): String = {
        val driver = getRaster.GetDriver()
        val ds = driver.CreateCopy(path, getRaster)
        ds.FlushCache()
        path
    }

    /**
      * Writes a raster to a byte array.
      *
      * @return
      *   A byte array containing the raster data.
      */
    override def writeToBytes(): Array[Byte] = {
        val isInMem = path.startsWith("/vsimem/")
        if (isInMem) {
            // Create a temporary directory to store the raster
            // This is needed because Files cannot read from /vsimem/ directly
            val randomID = UUID.randomUUID()
            val tmpDir = Files.createTempDirectory(s"mosaic_$randomID").toFile.getAbsolutePath
            val outPath = s"$tmpDir/raster_${uuid.toString.replace("-", "_")}.$getExtension"
            Files.createDirectories(Paths.get(outPath).getParent)
            writeToPath(outPath)
            val byteArray = Files.readAllBytes(Paths.get(outPath))
            Files.delete(Paths.get(outPath))
            byteArray
        } else {
            val byteArray = Files.readAllBytes(Paths.get(path))
            byteArray
        }
    }

    override def uuid: Long = _uuid

    override def getExtension: String = {
        val driver = getRaster.GetDriver()
        val extension = driver.GetMetadataItem("DMD_EXTENSION")
        extension
    }

    override def updateMetadata(key: String, uuid: Long): Unit = {
        val metadata = getRaster.GetMetadata_Dict().asInstanceOf[util.Hashtable[String, String]]
        metadata.put(key, uuid.toString)
        getRaster.SetMetadata(metadata)
    }

}

//noinspection ZeroIndexToHead
object MosaicRasterGDAL extends RasterReader {

    def apply(dataset: Dataset, path: String): MosaicRasterGDAL = {
        val uuid = Murmur3.hash64(path.getBytes())
        val raster = new MosaicRasterGDAL(uuid, dataset, path)
        raster
    }

    /**
      * Reads a raster from a file system path. Reads a subdataset if the path
      * is to a subdataset.
      *
      * @example
      *   Raster: path = "file:///path/to/file.tif" Subdataset: path =
      *   "file:///path/to/file.tif:subdataset"
      * @param inPath
      *   The path to the raster file.
      * @return
      *   A MosaicRaster object.
      */
    override def readRaster(inPath: String): MosaicRaster = {
        val path = inPath.replace("dbfs:/", "/dbfs/").replace("file:/", "/")
        val uuid = Murmur3.hash64(path.getBytes())
        // Subdatasets are paths with a colon in them.
        // We need to check for this condition and handle it.
        // Subdatasets paths are formatted as: "FORMAT:/path/to/file.tif:subdataset"
        val isSubdataset = path.contains(":")
        val readPath =
            if (isSubdataset) {
                val format :: filePath :: subdataset :: Nil = path.split(":").toList
                val isZip = filePath.endsWith(".zip")
                val vsiPrefix = if (isZip) "/vsizip/" else ""
                s"$format:$vsiPrefix$filePath:$subdataset"
            } else {
                val isZip = path.endsWith(".zip")
                val readPath = if (path.startsWith("/vsizip/")) path else if (isZip) s"/vsizip/$path" else path
                readPath
            }
        val dataset = gdal.Open(readPath, GA_ReadOnly)
        val raster = new MosaicRasterGDAL(uuid, dataset, path)
        raster
    }

    override def readRaster(contentBytes: Array[Byte]): MosaicRaster = {
        if (Option(contentBytes).isEmpty || contentBytes.isEmpty) {
            // Handle empty rasters, easy check to -1L as UUID for empty rasters
            new MosaicRasterGDAL(-1L, null, "")
        } else {
            // This is a temp UUID for purposes of reading the raster through GDAL from memory
            // The stable UUID is kept in metadata of the raster
            val uuid = Murmur3.hash64(UUID.randomUUID().toString.getBytes())
            val extension = "tif"
            val virtualPath = s"/vsimem/$uuid.$extension"
            gdal.FileFromMemBuffer(virtualPath, contentBytes)
            // Try reading as a virtual file, if that fails, read as a zipped virtual file
            val dataset = Option(gdal.Open(virtualPath)).getOrElse({
                val virtualPath = s"/vsimem/$uuid.zip"
                val zippedPath = s"/vsizip/$virtualPath"
                gdal.FileFromMemBuffer(virtualPath, contentBytes)
                gdal.Open(zippedPath)
            })
            val raster = new MosaicRasterGDAL(uuid, dataset, virtualPath)
            raster
        }
    }

    /**
      * Reads a raster band from a file system path. Reads a subdataset band if
      * the path is to a subdataset.
      *
      * @example
      *   Raster: path = "file:///path/to/file.tif" Subdataset: path =
      *   "file:///path/to/file.tif:subdataset"
      * @param path
      *   The path to the raster file.
      * @param bandIndex
      *   The band index to read.
      * @return
      *   A MosaicRaster object.
      */
    override def readBand(path: String, bandIndex: Int): MosaicRasterBand = {
        val raster = readRaster(path)
        raster.getBand(bandIndex)
    }

}
